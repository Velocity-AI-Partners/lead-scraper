"""
WellnessLiving scraper (IMA Worcester).

OAuth2 client_credentials -> bearer token -> paginated report/query (cid_report 689).

API quirks discovered via testing:
  - id_region and k_business must be QUERY PARAMS, not in JSON body
  - Rows are lists (not dicts), mapped via the a_field array
  - json_filter.o_member_status: [3] filters to prospects/leads
  - json_filter.o_date with dl_start/dl_end is required
  - o_since_date.dtl_date = creation date (used as staleness proxy)
"""

import logging
from datetime import date

import httpx

from app.config import settings
from app.schemas import Lead, ScrapeResponse
from app.utils.normalize import normalize_phone, normalize_name, days_since

log = logging.getLogger(__name__)

TOKEN_URL = "https://access.api.wellnessliving.io/oauth2/token"
API_BASE = "https://api.wellnessliving.io"
PAGE_SIZE = 50

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# Field index mapping (from a_field array, verified against live API)
F_UID = "uid"
F_FIRST = "field-general-2.text_name"
F_LAST = "field-general-1"
F_EMAIL = "field-general-3"
F_PHONE = "field-general-4"
F_CLIENT_TYPE = "text_client_type"
F_SINCE_DATE = "o_since_date.dtl_date"
F_NOTES = "o_note.text_note_list"


def _row_to_dict(row: list, fields: list[str]) -> dict:
    """Convert a positional row list into a dict keyed by field name."""
    return {fields[i]: row[i] for i in range(min(len(row), len(fields)))}


async def _get_token(client: httpx.AsyncClient) -> str:
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


async def _fetch_leads(client: httpx.AsyncClient, token: str) -> tuple[list[dict], list[str]]:
    """Paginate through report/query to get all prospects/leads."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": UA,
    }
    params = {"id_region": 1, "k_business": settings.wl_business_id}

    all_leads: list[dict] = []
    fields: list[str] = []
    errors: list[str] = []
    offset = 0

    while True:
        body = {
            "k_business": settings.wl_business_id,
            "cid_report": 689,
            "i_limit": PAGE_SIZE,
            "i_offset": offset,
            "is_backend": 1,
            "is_refresh": 0,
            "s_sort": "uid",
            "json_filter": {
                "o_member_status": [3],
                "o_date": {
                    "dl_start": "2020-01-01",
                    "dl_end": date.today().isoformat(),
                    "id_report_date": 4,
                },
                "o_search": "",
            },
        }

        try:
            resp = await client.post(
                f"{API_BASE}/v1/report/query",
                params=params,
                headers=headers,
                json=body,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            errors.append(f"report/query offset={offset}: {e}")
            break

        if data.get("status") != "ok":
            errors.append(f"API error: {data.get('message', data.get('text_error', 'unknown'))}")
            break

        # Capture field mapping from first response
        if not fields:
            fields = data.get("a_field", [])

        rows = data.get("a_row", [])
        for row in rows:
            if isinstance(row, list):
                all_leads.append(_row_to_dict(row, fields))

        log.info("WL: fetched %d rows at offset %d (total so far: %d)", len(rows), offset, len(all_leads))

        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return all_leads, errors


async def scrape_wellnessliving() -> ScrapeResponse:
    errors: list[str] = []
    leads: list[Lead] = []
    total_raw = 0

    async with httpx.AsyncClient(
        headers={"User-Agent": UA},
        follow_redirects=True,
        timeout=30,
    ) as client:
        token = await _get_token(client)
        all_rows, fetch_errors = await _fetch_leads(client, token)
        errors.extend(fetch_errors)

        # Deduplicate by uid
        seen: set[str] = set()
        unique_rows: list[dict] = []
        for row in all_rows:
            uid = str(row.get(F_UID, ""))
            if uid and uid not in seen:
                seen.add(uid)
                unique_rows.append(row)

        total_raw = len(unique_rows)
        log.info("WL: %d unique leads from %d total rows", total_raw, len(all_rows))

        for row in unique_rows:
            # Use o_since_date as staleness proxy (when they were added to the system)
            since_date = row.get(F_SINCE_DATE)
            days = days_since(since_date)

            # Skip fresh leads (added less than stale_days ago)
            if days is not None and days < settings.stale_days:
                continue

            leads.append(Lead(
                id=str(row.get(F_UID, "")),
                firstName=normalize_name(row.get(F_FIRST, "")),
                lastName=normalize_name(row.get(F_LAST, "")),
                email=row.get(F_EMAIL) or None,
                phone=normalize_phone(row.get(F_PHONE)),
                status=str(row.get(F_CLIENT_TYPE, "prospect")).lower(),
                lastContactDate=since_date,
                daysSinceContact=days,
                source="wellnessliving",
            ))

    return ScrapeResponse(
        leadCount=len(leads),
        leads=leads,
        errors=errors,
        metadata={"totalRaw": total_raw, "filteredTo": len(leads)},
    )
