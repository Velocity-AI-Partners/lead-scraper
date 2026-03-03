import logging
import traceback

from fastapi import FastAPI, Header, HTTPException

from app.config import settings
from app.schemas import ScrapeResponse
from app.scrapers.wellnessliving import scrape_wellnessliving
from app.scrapers.spark import scrape_spark
from app.scrapers.clubready import scrape_clubready
from app.bookers.models import (
    AvailabilityRequest,
    AvailabilityResponse,
    BookingRequest,
    BookingResponse,
    LOCATION_CONFIG,
)
from app.bookers.router import route_booking, route_availability
from app.bookers.clubready import get_member_info

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Lead Scraper", version="0.2.0", docs_url=None, redoc_url=None)


def _check_secret(secret: str | None):
    if secret != settings.api_secret:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/health")
async def health():
    return {"status": "ok"}


# ── Booking endpoints ──────────────────────────────────────────────


@app.post("/api/book", response_model=BookingResponse)
async def book(req: BookingRequest, x_external_api_secret: str | None = Header(None)):
    _check_secret(x_external_api_secret)
    try:
        return await route_booking(req)
    except Exception as e:
        log.error("Booking failed: %s\n%s", e, traceback.format_exc())
        return BookingResponse(
            success=False, message=f"Internal error: {e}", errors=[str(e)]
        )


@app.post("/api/availability", response_model=AvailabilityResponse)
async def availability(
    req: AvailabilityRequest, x_external_api_secret: str | None = Header(None)
):
    _check_secret(x_external_api_secret)
    try:
        return await route_availability(req)
    except Exception as e:
        log.error("Availability check failed: %s\n%s", e, traceback.format_exc())
        return AvailabilityResponse(
            location_slug=req.location_slug, date=req.date, errors=[str(e)]
        )


@app.get("/api/locations")
async def locations():
    """Return supported locations and their CRM types (no auth required)."""
    return {
        slug: {"name": cfg["name"], "crm": cfg["crm"], "has_availability": cfg["crm"] == "clubready"}
        for slug, cfg in LOCATION_CONFIG.items()
    }


@app.get("/api/member-info")
async def member_info(
    location: str,
    member_id: str,
    x_external_api_secret: str | None = Header(None),
):
    """Fetch ClubReady member profile + credits. No auth required (public info)."""
    cfg = LOCATION_CONFIG.get(location)
    if not cfg:
        raise HTTPException(status_code=404, detail=f"Unknown location: {location}")
    if cfg["crm"] != "clubready":
        raise HTTPException(status_code=400, detail="Member info only supported for ClubReady locations")

    try:
        result = await get_member_info(
            store_id=cfg["store_id"],
            username=cfg["cr_username"],
            password=cfg["cr_password"],
            member_id=member_id,
        )
        return result
    except Exception as e:
        log.error("Member info failed: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to fetch member info: {e}")


# ── Scraper endpoints (existing) ──────────────────────────────────


@app.post("/scrape/wellnessliving", response_model=ScrapeResponse)
async def scrape_wl(x_external_api_secret: str | None = Header(None)):
    _check_secret(x_external_api_secret)
    try:
        return await scrape_wellnessliving()
    except Exception as e:
        log.error("WellnessLiving scrape failed: %s\n%s", e, traceback.format_exc())
        return ScrapeResponse(leadCount=0, leads=[], errors=[str(e)], metadata={})


@app.post("/scrape/spark", response_model=ScrapeResponse)
async def scrape_sp(x_external_api_secret: str | None = Header(None)):
    _check_secret(x_external_api_secret)
    try:
        return await scrape_spark()
    except Exception as e:
        log.error("Spark scrape failed: %s\n%s", e, traceback.format_exc())
        return ScrapeResponse(leadCount=0, leads=[], errors=[str(e)], metadata={})


@app.post("/scrape/clubready", response_model=ScrapeResponse)
async def scrape_cr(x_external_api_secret: str | None = Header(None)):
    _check_secret(x_external_api_secret)
    try:
        return await scrape_clubready()
    except Exception as e:
        log.error("ClubReady scrape failed: %s\n%s", e, traceback.format_exc())
        return ScrapeResponse(leadCount=0, leads=[], errors=[str(e)], metadata={})
