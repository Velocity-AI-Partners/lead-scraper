"""
ClubReady booking via api.clubready.com (Azure APIM).

Auth flow:
  1. 3-step cookie auth (login.clubready.com -> loginselector -> Security/Login)
  2. POST OAuthBridge/Token (cookies -> Bearer JWT, 30min TTL)
  3. Call api.clubready.com with Bearer + APIM subscription key

Verified endpoints:
  - GET  /scheduling/v1/clubs/{id}/services/schedule (booked slots)
  - GET  /scheduling/v1/clubs/{id}/instructors/{id}/gross-availability (open blocks)
  - GET  /users/v1/clubs/{id}/customers?search=X (find customer)
  - POST /scheduling/v1/bookings/services (create booking, clubId in body)
  - PATCH /scheduling/v1/clubs/{id}/bookings/cancel (cancel, bookingIds in body)

Booking payload (confirmed from SPA JS + live test):
  {userId, clubId, bookingStartTimeUtc, instructorId, sessionSizeId,
   sendEmail, sendSms, allowOverBooking, allowWaitList}
"""

import base64
import json
import logging
import re
import time
from datetime import datetime, timedelta

import httpx

from app.bookers.models import (
    AvailabilitySlot,
    BookingRequest,
    BookingResponse,
)

log = logging.getLogger(__name__)

APIM_SUB_KEY = "b7790d8530a34e0d9f68d0fb360886a4"
API_BASE = "https://api.clubready.com"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# Token cache: store_id -> (bearer_token, expires_at)
_token_cache: dict[str, tuple[str, float]] = {}


def _decode_jwt_payload(token: str) -> dict:
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(payload_b64))
    except Exception:
        return {}


async def _get_bearer_token(store_id: str, username: str, password: str) -> str:
    """
    Full auth flow: 3-step cookies -> OAuthBridge -> Bearer JWT.
    Caches token per store_id until 5 min before expiry.
    """
    cached = _token_cache.get(store_id)
    if cached and cached[1] > time.time():
        return cached[0]

    async with httpx.AsyncClient(headers={"User-Agent": UA}, timeout=30) as client:
        # Step 1: Login to get JWT token
        resp = await client.post(
            "https://login.clubready.com/Security/Login",
            data={"username": username, "pw": password, "inst": "1"},
            follow_redirects=True,
        )
        body = resp.text
        token_match = re.search(r'"Token"\s*:\s*"([^"]+)"', body)
        if not token_match:
            jwts = re.findall(
                r"eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}",
                body,
            )
            if not jwts:
                raise RuntimeError(f"CR auth step 1: no token. Status={resp.status_code}")
            token = jwts[0]
        else:
            token = token_match.group(1)

        payload = _decode_jwt_payload(token)
        user_id = str(payload.get("UserId", payload.get("userId", payload.get("sub", ""))))
        log.info("CR auth step 1: UserId=%s, store=%s", user_id, store_id)

        # Step 2: Store selector
        resp = await client.post(
            "https://www.clubready.com/login/loginselector",
            data={"Token": token, "CoreTypeId": "1", "CoreId": "1", "StoreId": store_id},
            follow_redirects=True,
        )
        log.info("CR auth step 2: status=%d", resp.status_code)

        # Step 3: Final login
        resp = await client.post(
            "https://www.clubready.com/Security/Login",
            data={"CoreTypeId": "1", "CoreId": "1", "Token": token, "UID": user_id},
            follow_redirects=True,
        )
        log.info("CR auth step 3: status=%d", resp.status_code)

        # Step 4: OAuthBridge -> Bearer JWT
        resp = await client.post(
            "https://app.clubready.com/OAuthBridge/Token",
            headers={
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://scheduling.clubready.com",
                "Referer": "https://scheduling.clubready.com/",
            },
        )
        if resp.status_code != 200:
            raise RuntimeError(f"OAuthBridge failed: {resp.status_code} {resp.text[:200]}")

        data = resp.json()
        access_token = data["access_token"]
        expires_in = data.get("expires_in", 1800)

        # Cache with 5-min safety margin
        _token_cache[store_id] = (access_token, time.time() + expires_in - 300)
        log.info("CR auth complete: Bearer token cached for store %s (%ds TTL)", store_id, expires_in)

        return access_token


def _api_headers(bearer: str) -> dict:
    return {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer}",
        "ocp-apim-subscription-key": APIM_SUB_KEY,
        "Origin": "https://scheduling.clubready.com",
        "Referer": "https://scheduling.clubready.com/",
        "User-Agent": UA,
    }


async def get_availability(
    store_id: str,
    username: str,
    password: str,
    date: str,
) -> list[AvailabilitySlot]:
    """
    Get booked slots for a date (shows what's scheduled, including instructor + service).
    Uses GET /scheduling/v1/clubs/{id}/services/schedule (paginated).
    """
    bearer = await _get_bearer_token(store_id, username, password)
    headers = _api_headers(bearer)

    start_utc = f"{date}T05:00:00.000Z"
    dt = datetime.fromisoformat(date)
    next_day = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
    end_utc = f"{next_day}T04:59:59.999Z"

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{API_BASE}/scheduling/v1/clubs/{store_id}/services/schedule",
            params={
                "startDateTimeUtc": start_utc,
                "endDateTimeUtc": end_utc,
                "page": 1,
                "pageSize": 100,
            },
            headers=headers,
        )
        if resp.status_code != 200:
            log.error("CR availability failed: %d %s", resp.status_code, resp.text[:200])
            return []

        data = resp.json()

    slots: list[AvailabilitySlot] = []
    items = data if isinstance(data, list) else data.get("data", data.get("items", []))

    for item in items:
        instr = item.get("instructor", {}) or {}
        instr_name = ""
        if instr.get("firstName"):
            instr_name = f"{instr['firstName']} {instr.get('lastName', '')}".strip()

        slots.append(AvailabilitySlot(
            start_time=item.get("startDateTimeUtc", ""),
            end_time=item.get("endDateTimeUtc", ""),
            instructor_name=instr_name,
            instructor_id=str(instr.get("id", "")),
            service_name=item.get("serviceName", ""),
            service_id=str(item.get("serviceId", "")),
            spots_available=None,
        ))

    log.info("CR availability: %d slots for store %s on %s", len(slots), store_id, date)
    return slots


async def find_customer(
    store_id: str, username: str, password: str, search: str
) -> dict | None:
    """Search for a customer by name/email/phone. Returns first match or None."""
    bearer = await _get_bearer_token(store_id, username, password)
    headers = _api_headers(bearer)

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{API_BASE}/users/v1/clubs/{store_id}/customers",
            params={"search": search},
            headers=headers,
        )
        if resp.status_code != 200:
            log.warning("CR customer search failed: %d", resp.status_code)
            return None

        data = resp.json()

    results = data if isinstance(data, list) else data.get("items", data.get("data", []))
    if results:
        return results[0]
    return None


async def cancel_booking(store_id: str, username: str, password: str, booking_id: int) -> bool:
    """Cancel a booking via PATCH /scheduling/v1/clubs/{id}/bookings/cancel."""
    bearer = await _get_bearer_token(store_id, username, password)
    headers = _api_headers(bearer)

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.patch(
            f"{API_BASE}/scheduling/v1/clubs/{store_id}/bookings/cancel",
            headers=headers,
            params={"StatusId": 10},  # 10 = CancelledByAdmin
            json={
                "bookingIds": [booking_id],
                "sendSms": False,
                "sendEmail": False,
            },
        )
        if resp.status_code == 200:
            log.info("CR: booking %d cancelled", booking_id)
            return True
        log.error("CR: cancel failed: %d %s", resp.status_code, resp.text[:200])
        return False


async def create_booking(req: BookingRequest, loc_config: dict) -> BookingResponse:
    """
    Create a booking in ClubReady.

    POST /scheduling/v1/bookings/services (clubId in body, NOT in URL path).

    Steps:
      1. Auth -> bearer token
      2. Find customer by email/phone/name
      3. Fetch schedule to find matching slot (for sessionSizeId, instructorId)
      4. POST booking
    """
    store_id = loc_config["store_id"]
    username = loc_config["cr_username"]
    password = loc_config["cr_password"]

    try:
        bearer = await _get_bearer_token(store_id, username, password)
    except Exception as e:
        return BookingResponse(
            success=False,
            message=f"ClubReady auth failed: {e}",
            errors=[str(e)],
        )

    headers = _api_headers(bearer)

    # Step 1: Resolve customer ID (prefer direct member_id, fall back to search)
    customer_id = None
    if req.member_id:
        customer_id = req.member_id
        log.info("CR: using provided member_id=%s", customer_id)
    else:
        search_term = req.customer_email or req.customer_phone or req.customer_name
        if search_term:
            customer = await find_customer(store_id, username, password, search_term)
            if customer:
                customer_id = str(customer.get("userId", customer.get("id", "")))
                log.info("CR: found customer %s for '%s'", customer_id, search_term)

    # Step 2: Get schedule to find a slot matching the requested time
    schedule_data = []
    try:
        dt = datetime.fromisoformat(req.requested_date)
        next_day = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{API_BASE}/scheduling/v1/clubs/{store_id}/services/schedule",
                params={
                    "startDateTimeUtc": f"{req.requested_date}T05:00:00.000Z",
                    "endDateTimeUtc": f"{next_day}T04:59:59.999Z",
                    "page": 1,
                    "pageSize": 100,
                },
                headers=headers,
            )
            if resp.status_code == 200:
                raw = resp.json()
                schedule_data = raw if isinstance(raw, list) else raw.get("data", raw.get("items", []))
    except Exception as e:
        log.warning("CR: failed to fetch schedule: %s", e)

    # Match slot by time (and optionally instructor)
    matching_item = None
    for item in schedule_data:
        start_utc = item.get("startDateTimeUtc", "")
        if req.requested_time in start_utc:
            if req.instructor:
                instr = item.get("instructor", {}) or {}
                instr_name = f"{instr.get('firstName', '')} {instr.get('lastName', '')}".strip()
                if req.instructor.lower() not in instr_name.lower():
                    continue
            matching_item = item
            break

    # Step 3: Build booking payload
    # Confirmed fields: userId, clubId, bookingStartTimeUtc, instructorId,
    #   sessionSizeId, sendEmail, sendSms, allowOverBooking, allowWaitList
    booking_body: dict = {
        "clubId": int(store_id),
        "sendEmail": False,
        "sendSms": False,
        "allowOverBooking": False,
        "allowWaitList": False,
    }

    if customer_id:
        booking_body["userId"] = int(customer_id)

    if matching_item:
        booking_body["bookingStartTimeUtc"] = matching_item["startDateTimeUtc"]
        instr = matching_item.get("instructor", {}) or {}
        if instr.get("id"):
            booking_body["instructorId"] = instr["id"]
        if matching_item.get("serviceId"):
            booking_body["serviceId"] = matching_item["serviceId"]
        if matching_item.get("sessionSizeId"):
            booking_body["sessionSizeId"] = matching_item["sessionSizeId"]
    else:
        # No exact slot match: construct from request data
        booking_body["bookingStartTimeUtc"] = f"{req.requested_date}T{req.requested_time}:00.000Z"
        log.warning("CR: no matching slot found, using raw time")

    # Step 4: POST booking
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{API_BASE}/scheduling/v1/bookings/services",
                headers=headers,
                json=booking_body,
            )

            if resp.status_code == 200:
                data = resp.json()
                booking_id = str(data.get("bookingId", ""))
                log.info("CR: booking created! ID=%s", booking_id)
                return BookingResponse(
                    success=True,
                    booking_id=booking_id,
                    customer_id=customer_id,
                    message="Booking confirmed in ClubReady",
                )
            elif resp.status_code == 400:
                # Business validation error (e.g. "UserBooked", "NoCredits")
                error_text = resp.text[:500]
                log.warning("CR: booking rejected: %s", error_text)
                return BookingResponse(
                    success=False,
                    message=f"Booking not available: {error_text}",
                    errors=[error_text],
                )
            else:
                error_text = resp.text[:500]
                log.error("CR: booking POST failed: %d %s", resp.status_code, error_text)
                return BookingResponse(
                    success=False,
                    message=f"ClubReady booking failed ({resp.status_code})",
                    errors=[error_text],
                )
    except Exception as e:
        return BookingResponse(
            success=False,
            message=f"ClubReady booking error: {e}",
            errors=[str(e)],
        )


async def get_member_info(
    store_id: str, username: str, password: str, member_id: str
) -> dict:
    """
    Fetch member profile + available credits from ClubReady.

    Returns dict with: name, firstName, lastName, status, membershipType,
    isMember, credits (count), creditDetails (list of credit names).
    """
    bearer = await _get_bearer_token(store_id, username, password)
    headers = _api_headers(bearer)

    result: dict = {
        "name": None,
        "firstName": None,
        "lastName": None,
        "status": None,
        "membershipType": None,
        "isMember": False,
        "credits": None,
        "creditDetails": [],
    }

    async with httpx.AsyncClient(timeout=20) as client:
        # 1. User profile
        resp = await client.get(
            f"{API_BASE}/users/v1/users/{member_id}",
            params={"ReturnKeyNote": "true"},
            headers=headers,
        )
        if resp.status_code == 200:
            d = resp.json()
            result["firstName"] = d.get("FirstName")
            result["lastName"] = d.get("LastName")
            fn = (d.get("FirstName") or "")
            ln = (d.get("LastName") or "")
            result["name"] = f"{fn} {ln}".strip() or None
            result["status"] = d.get("CustomerStatus")
            result["membershipType"] = d.get("MembershipTypeName")
            result["isMember"] = bool(d.get("Member"))
        else:
            log.warning("CR member-info user lookup failed: %d", resp.status_code)
            return result

        # 2. Available credits
        resp = await client.get(
            f"{API_BASE}/scheduling/v1/clubs/{store_id}/users/{member_id}/credits",
            headers=headers,
        )
        if resp.status_code == 200:
            cd = resp.json()
            result["credits"] = cd.get("totalRecords", 0)
            result["creditDetails"] = [
                c.get("name", "") for c in cd.get("data", [])
            ]
        else:
            log.warning("CR member-info credits lookup failed: %d", resp.status_code)

    log.info(
        "CR member-info: %s (%s) credits=%s",
        result["name"], result["status"], result["credits"],
    )
    return result
