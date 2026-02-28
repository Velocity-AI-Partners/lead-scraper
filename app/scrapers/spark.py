"""
Spark Membership scraper (IMA Westborough).

ASP.NET WebForms auth, then hits Contacts.ashx JSON endpoint directly.
No HTML parsing needed (the grid data comes as clean JSON).

Fields per contact: contactID, firstName, lastName, emailAddress, mobilePhone,
phone, contactType (L/P/T/I), dateEntered, lastSeenDaysAgo, etc.
"""

import logging
import re

import httpx

from app.config import settings
from app.schemas import Lead, ScrapeResponse
from app.utils.normalize import normalize_phone, normalize_name

log = logging.getLogger(__name__)

BASE_URL = "https://app.sparkmembership.com"
LOGIN_URL = f"{BASE_URL}/login.aspx"
CONTACTS_API = f"{BASE_URL}/Contacts.ashx"

# Contact types to scrape: L=Leads, P=Prospects, T=Trials, I=Inquiries
LEAD_TYPES = ["L", "P", "T", "I"]

CONTACT_TYPE_LABELS = {"L": "lead", "P": "prospect", "T": "trial", "I": "inquiry"}


async def _login(client: httpx.AsyncClient) -> None:
    """ASP.NET WebForms login: GET login page for ViewState, POST credentials."""
    resp = await client.get(LOGIN_URL, follow_redirects=True, timeout=30)
    html = resp.text

    # Extract hidden fields
    viewstate: dict[str, str] = {}
    for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION",
                 "__EVENTTARGET", "__EVENTARGUMENT"):
        match = re.search(rf'id="{name}"\s+value="([^"]*)"', html)
        if match:
            viewstate[name] = match.group(1)

    hlogin_match = re.search(r'id="hLogin"[^>]*value="([^"]*)"', html)

    if not viewstate.get("__VIEWSTATE"):
        raise RuntimeError("Could not extract __VIEWSTATE from Spark login page")

    form_data = {
        **viewstate,
        "hLogin": hlogin_match.group(1) if hlogin_match else "",
        "txtEmail": settings.spark_email,
        "txtPass": settings.spark_password,
        "btnLogin": "Login",
    }

    resp = await client.post(LOGIN_URL, data=form_data, follow_redirects=True, timeout=30)

    if "login.aspx" in str(resp.url).lower() and "btnLogin" in resp.text:
        raise RuntimeError("Spark login failed: still on login page after POST")

    log.info("Spark: logged in, cookies: %s", list(client.cookies.keys()))


async def _fetch_contacts(client: httpx.AsyncClient, contact_type: str) -> list[dict]:
    """Fetch contacts of a given type via the Contacts.ashx JSON API."""
    resp = await client.get(
        CONTACTS_API,
        params={"contactType": f"'{contact_type}',"},
        timeout=60,
    )

    if resp.status_code != 200:
        log.warning("Spark: Contacts.ashx contactType=%s returned %d", contact_type, resp.status_code)
        return []

    data = resp.json()
    if not isinstance(data, list):
        log.warning("Spark: unexpected response type %s for contactType=%s", type(data), contact_type)
        return []

    log.info("Spark: %d contacts for contactType=%s", len(data), contact_type)
    return data


async def scrape_spark() -> ScrapeResponse:
    errors: list[str] = []
    leads: list[Lead] = []
    total_raw = 0

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
    ) as client:
        try:
            await _login(client)
        except Exception as e:
            return ScrapeResponse(
                leadCount=0, leads=[], errors=[f"Login failed: {e}"], metadata={}
            )

        all_contacts: list[dict] = []
        for ct in LEAD_TYPES:
            try:
                contacts = await _fetch_contacts(client, ct)
                all_contacts.extend(contacts)
            except Exception as e:
                errors.append(f"contactType={ct}: {e}")

        # Deduplicate by contactID
        seen: set[int] = set()
        unique: list[dict] = []
        for c in all_contacts:
            cid = c.get("contactID")
            if cid and cid not in seen:
                seen.add(cid)
                unique.append(c)

        total_raw = len(unique)

        for c in unique:
            days_ago = c.get("lastSeenDaysAgo", 0) or 0

            # Only include stale contacts (last seen > threshold)
            if days_ago < settings.stale_days:
                continue

            # Pick best phone: mobilePhone > phone > workPhone
            phone_raw = c.get("mobilePhone") or c.get("phone") or c.get("workPhone")

            ct_code = c.get("contactType", "L")

            leads.append(Lead(
                id=str(c.get("contactID", "")),
                firstName=normalize_name(c.get("firstName", "")),
                lastName=normalize_name(c.get("lastName", "")),
                email=c.get("emailAddress") or None,
                phone=normalize_phone(phone_raw),
                status=CONTACT_TYPE_LABELS.get(ct_code, "lead"),
                lastContactDate=c.get("dateEntered"),
                daysSinceContact=days_ago,
                source="spark",
            ))

    return ScrapeResponse(
        leadCount=len(leads),
        leads=leads,
        errors=errors,
        metadata={"totalRaw": total_raw, "filteredTo": len(leads)},
    )
