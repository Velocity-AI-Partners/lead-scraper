"""
ClubReady scraper (SZ Westborough).

3-step cookie chain auth + A-Z brute-force QuickSearch.
Filters to leads/prospects during scraping (skips members).
Auth flow matches the working n8n Member_List_Scraper_Updated workflow exactly.
"""

import asyncio
import base64
import json
import logging
import re
import string

import httpx

from app.config import settings
from app.schemas import Lead, ScrapeResponse
from app.utils.normalize import normalize_phone, normalize_name, days_since

log = logging.getLogger(__name__)

LOGIN_URL = "https://login.clubready.com/Security/Login"
SELECTOR_URL = "https://www.clubready.com/login/loginselector"
SECURITY_URL = "https://www.clubready.com/Security/Login"
QUICKSEARCH_URL = "https://app.clubready.com/Users/Lookup/QuickSearch"

BATCH_SIZE = 25
MAX_RETRIES = 3
RETRY_DELAY = 1.0

LEAD_STATUSES = {"lead", "prospect", "inquiry", "trial", "intro", "guest"}

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def _decode_jwt_payload(token: str) -> dict:
    """Decode the payload section of a JWT (no signature verification needed)."""
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    payload_b64 = parts[1]
    # Add padding if needed
    payload_b64 += "=" * (4 - len(payload_b64) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(payload_b64))
    except Exception:
        return {}


def _extract_cookies(resp: httpx.Response) -> dict[str, str]:
    """Extract cookies from Set-Cookie headers."""
    cookies: dict[str, str] = {}
    raw = resp.headers.get_list("set-cookie")
    for header in raw:
        match = re.match(r"^([^=]+)=([^;]*)", header.strip())
        if match:
            cookies[match.group(1).strip()] = match.group(2)
    return cookies


async def _auth(client: httpx.AsyncClient) -> str:
    """
    3-step ClubReady login. Returns the merged cookie string for QuickSearch.

    Step 1: POST login.clubready.com with username/pw/inst -> get JWT token
    Step 2: POST loginselector with Token/StoreId -> get session cookies
    Step 3: POST Security/Login with Token/UID + step2 cookies -> get final cookies
    """
    headers = {"User-Agent": UA}

    # Step 1: Login to get JWT token
    resp = await client.post(
        LOGIN_URL,
        data={"username": settings.cr_username, "pw": settings.cr_password, "inst": "1"},
        headers=headers,
        follow_redirects=False,
        timeout=30,
    )
    # Follow redirect manually if needed
    if resp.status_code in (301, 302, 303):
        location = resp.headers.get("location", "")
        if location:
            resp = await client.get(location, headers=headers, follow_redirects=True, timeout=30)

    body = resp.text

    # Extract token from response body
    token_match = re.search(r'"Token"\s*:\s*"([^"]+)"', body)
    if not token_match:
        raise RuntimeError(f"ClubReady step 1: no Token in response. Status={resp.status_code}, body preview: {body[:300]}")

    token = token_match.group(1)

    # Decode JWT to get UserId and StoreId
    payload = _decode_jwt_payload(token)
    user_id = str(payload.get("UserId", payload.get("userId", payload.get("sub", ""))))

    # Auto-detect StoreId from JWT (field name varies: storeId, StoreId, Stores)
    store_id = settings.cr_store_id
    if not store_id:
        if payload.get("storeId"):
            store_id = str(payload["storeId"])
        elif payload.get("StoreId"):
            store_id = str(payload["StoreId"])
        elif payload.get("Stores") and isinstance(payload["Stores"], list):
            stores = payload["Stores"]
            if len(stores) == 1:
                store_id = str(stores[0].get("StoreId", stores[0].get("Id", "")))
            else:
                # Find Westborough
                for s in stores:
                    name = str(s.get("Name", s.get("StoreName", ""))).lower()
                    if "westborough" in name:
                        store_id = str(s.get("StoreId", s.get("Id", "")))
                        break
                if not store_id:
                    store_id = str(stores[0].get("StoreId", stores[0].get("Id", "")))

    if not store_id:
        raise RuntimeError(f"ClubReady step 1: could not determine StoreId. JWT payload keys: {list(payload.keys())}")

    log.info("ClubReady: step 1 complete. UserId=%s, StoreId=%s, token=%s...", user_id, store_id, token[:20])

    # Step 2: POST to loginselector with Token + StoreId
    resp = await client.post(
        SELECTOR_URL,
        data={"Token": token, "CoreTypeId": "1", "CoreId": "1", "StoreId": store_id},
        headers=headers,
        follow_redirects=False,
        timeout=30,
    )
    step2_cookies = _extract_cookies(resp)
    step2_cookie_str = "; ".join(f"{k}={v}" for k, v in step2_cookies.items())

    log.info("ClubReady: step 2 complete. %d cookies from response", len(step2_cookies))

    # Step 3: POST to Security/Login with Token + UID, passing step 2 cookies
    step3_headers = {**headers, "Cookie": step2_cookie_str}
    resp = await client.post(
        SECURITY_URL,
        data={"CoreTypeId": "1", "CoreId": "1", "Token": token, "UID": user_id},
        headers=step3_headers,
        follow_redirects=False,
        timeout=30,
    )
    step3_cookies = _extract_cookies(resp)

    # Merge all cookies
    all_cookies = {**step2_cookies, **step3_cookies}
    cookie_str = "; ".join(f"{k}={v}" for k, v in all_cookies.items())

    log.info("ClubReady: step 3 complete. %d total session cookies", len(all_cookies))
    return cookie_str


async def _quicksearch(
    client: httpx.AsyncClient, prefix: str, cookies: str, retries: int = MAX_RETRIES
) -> list[dict]:
    """Run a single QuickSearch query with retry."""
    for attempt in range(retries):
        try:
            resp = await client.get(
                QUICKSEARCH_URL,
                params={"searchText": prefix, "searchType": "1"},
                headers={"User-Agent": UA, "Cookie": cookies},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data if isinstance(data, list) else []
            if resp.status_code == 429:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return []
        except Exception:
            if attempt < retries - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
    return []


def _is_cr_lead(member: dict) -> bool:
    """Check if a ClubReady QuickSearch result is a lead/prospect."""
    status_text = str(member.get("customerStatusText", "")).lower().strip()
    if status_text in LEAD_STATUSES:
        return True
    status_id = member.get("customerStatus")
    if status_id in (3, "3"):
        return True
    return False


def _extract_cr_contact_date(member: dict) -> str | None:
    """Extract last contact/activity date from ClubReady QuickSearch fields."""
    for field in (
        "lastContactDate", "lastActivityDate", "lastVisitDate",
        "lastModifiedDate", "createdDate", "addedDate",
    ):
        val = member.get(field)
        if val and str(val).strip() not in ("", "null", "None", "0"):
            return str(val).strip()
    return None


async def scrape_clubready() -> ScrapeResponse:
    errors: list[str] = []
    lead_map: dict[int, dict] = {}
    queries = 0
    capped: list[str] = []
    letters = string.ascii_lowercase

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            cookie_str = await _auth(client)
        except Exception as e:
            return ScrapeResponse(
                leadCount=0, leads=[], errors=[f"Auth failed: {e}"], metadata={}
            )

        # Pass 1: 3-letter prefixes (26^2 = 676 per first letter, 17,576 total)
        for c1 in letters:
            prefixes = [c1 + c2 + c3 for c2 in letters for c3 in letters]

            for i in range(0, len(prefixes), BATCH_SIZE):
                batch = prefixes[i : i + BATCH_SIZE]
                results = await asyncio.gather(
                    *[_quicksearch(client, p, cookie_str) for p in batch]
                )

                for j, result_list in enumerate(results):
                    for member in result_list:
                        uid = member.get("userId")
                        if uid and uid not in lead_map and _is_cr_lead(member):
                            lead_map[uid] = member
                    if len(result_list) >= 50:
                        capped.append(batch[j])

                queries += len(batch)

            log.info(
                "ClubReady: letter '%s' done, leads so far: %d, queries: %d, capped: %d",
                c1, len(lead_map), queries, len(capped),
            )

        # Pass 2: drill capped prefixes to 4 letters
        if capped:
            log.info("ClubReady: drilling %d capped prefixes to 4 letters", len(capped))
            drill_prefixes = [base + c4 for base in capped for c4 in letters]

            for i in range(0, len(drill_prefixes), BATCH_SIZE):
                batch = drill_prefixes[i : i + BATCH_SIZE]
                results = await asyncio.gather(
                    *[_quicksearch(client, p, cookie_str) for p in batch]
                )
                for result_list in results:
                    for member in result_list:
                        uid = member.get("userId")
                        if uid and uid not in lead_map and _is_cr_lead(member):
                            lead_map[uid] = member
                queries += len(batch)

        log.info("ClubReady: scan complete. %d leads, %d queries", len(lead_map), queries)

        # Convert to Lead objects with staleness filter
        leads: list[Lead] = []
        for uid, member in lead_map.items():
            last_contact = _extract_cr_contact_date(member)
            days = days_since(last_contact)

            # Only include stale leads
            if days is not None and days < settings.stale_days:
                continue

            first = member.get("firstName", member.get("name", ""))
            last = member.get("lastName", "")

            # Handle combined name field
            if not last and first and " " in first:
                parts = first.split(None, 1)
                first = parts[0]
                last = parts[1] if len(parts) > 1 else ""

            leads.append(Lead(
                id=str(uid),
                firstName=normalize_name(first),
                lastName=normalize_name(last),
                email=member.get("email"),
                phone=normalize_phone(member.get("phone", member.get("cellPhone"))),
                status=str(member.get("customerStatusText", "lead")).lower(),
                lastContactDate=last_contact,
                daysSinceContact=days,
                source="clubready",
            ))

    return ScrapeResponse(
        leadCount=len(leads),
        leads=leads,
        errors=errors,
        metadata={
            "queriesRun": queries,
            "totalRaw": len(lead_map),
            "filteredTo": len(leads),
            "cappedPrefixes": len(capped),
        },
    )
