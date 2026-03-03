"""
WellnessLiving booking (IMA Worcester).

Auth: OAuth2 client_credentials (reuses pattern from scrapers/wellnessliving.py).
Booking: /v1/appointment/book/finish (with is_try=1 for dry run).
"""

import logging

try:
    from curl_cffi.requests import AsyncSession as _CurlSession

    USE_CURL = True
except ImportError:
    _CurlSession = None  # type: ignore
    USE_CURL = False

import httpx

from app.config import settings
from app.bookers.models import BookingRequest, BookingResponse

log = logging.getLogger(__name__)

TOKEN_URL = "https://access.api.wellnessliving.io/oauth2/token"
API_BASE = "https://api.wellnessliving.io"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


async def _get_token(client) -> str:
    """Get OAuth2 bearer token."""
    resp = await client.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": settings.wl_client_id,
            "client_secret": settings.wl_client_secret,
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _get_client():
    """Get an HTTP client (curl_cffi for Cloudflare bypass, or httpx fallback)."""
    if USE_CURL:
        return _CurlSession(impersonate="chrome")
    return httpx.AsyncClient(
        headers={"User-Agent": UA}, follow_redirects=True, timeout=30
    )


async def find_user(client, token: str, search: str) -> dict | None:
    """Search for a user/member by name, email, or phone."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": UA,
    }
    params = {"id_region": 1, "k_business": settings.wl_business_id}

    try:
        resp = await client.get(
            f"{API_BASE}/v1/user",
            params={**params, "text_search": search},
            headers=headers,
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            users = data.get("a_user", data.get("a_data", []))
            if isinstance(users, dict):
                # WL sometimes returns dict keyed by uid
                users = list(users.values())
            if users:
                return users[0] if isinstance(users[0], dict) else None
    except Exception as e:
        log.warning("WL user search failed: %s", e)

    return None


async def create_booking(req: BookingRequest, loc_config: dict) -> BookingResponse:
    """
    Book an appointment in WellnessLiving.

    Uses /v1/appointment/book/finish endpoint.
    Set is_try=1 for a dry run to validate without creating.
    """
    client = _get_client()

    try:
        async with client as c:
            token = await _get_token(c)
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "User-Agent": UA,
            }
            base_params = {
                "id_region": 1,
                "k_business": settings.wl_business_id,
            }

            # Step 1: Resolve user ID (prefer direct member_id, fall back to search)
            uid = None
            if req.member_id:
                uid = req.member_id
                log.info("WL: using provided member_id=%s as uid", uid)
            else:
                search_term = req.customer_email or req.customer_phone or req.customer_name
                if search_term:
                    user = await find_user(c, token, search_term)
                    if user:
                        uid = user.get("uid", user.get("k_user"))
                        log.info("WL: found user uid=%s for '%s'", uid, search_term)

            # Step 2: Book appointment
            book_body = {
                "k_business": settings.wl_business_id,
                "k_location": settings.wl_location_id,
                "dt_date": f"{req.requested_date} {req.requested_time}:00",
                "is_try": 0,  # 0 = real booking, 1 = dry run
            }

            if uid:
                book_body["uid"] = uid

            if req.customer_name:
                parts = req.customer_name.split(None, 1)
                book_body["s_firstname"] = parts[0]
                book_body["s_lastname"] = parts[1] if len(parts) > 1 else ""

            if req.customer_email:
                book_body["s_email"] = req.customer_email
            if req.customer_phone:
                book_body["s_phone"] = req.customer_phone
            if req.notes:
                book_body["s_note"] = req.notes

            resp = await c.post(
                f"{API_BASE}/v1/appointment/book/finish",
                params=base_params,
                headers=headers,
                json=book_body,
                timeout=30,
            )

            if resp.status_code == 200:
                data = resp.json()
                status = data.get("status", "")

                if status == "ok" or data.get("k_appointment"):
                    appointment_id = str(data.get("k_appointment", ""))
                    log.info("WL: appointment booked! id=%s", appointment_id)
                    return BookingResponse(
                        success=True,
                        booking_id=appointment_id or None,
                        customer_id=str(uid) if uid else None,
                        message="Booking confirmed in WellnessLiving",
                    )
                else:
                    error_msg = data.get("s_message", data.get("text_error", str(data)))
                    log.error("WL: booking returned error: %s", error_msg)
                    return BookingResponse(
                        success=False,
                        message=f"WellnessLiving error: {error_msg}",
                        errors=[str(data)],
                    )

            error_text = resp.text[:500]
            log.error("WL: booking failed: %d %s", resp.status_code, error_text)
            return BookingResponse(
                success=False,
                message=f"WellnessLiving booking failed ({resp.status_code})",
                errors=[error_text],
            )

    except Exception as e:
        log.error("WL booking error: %s", e)
        return BookingResponse(
            success=False,
            message=f"WellnessLiving booking error: {e}",
            errors=[str(e)],
        )
