"""
Booking router: dispatches to the correct CRM booker based on location slug.
Logs all booking attempts to Supabase (booking_requests table).
"""

import logging
from datetime import datetime, timezone

import httpx

from app.config import settings
from app.bookers.models import (
    LOCATION_CONFIG,
    AvailabilityRequest,
    AvailabilityResponse,
    BookingRequest,
    BookingResponse,
)
from app.bookers import clubready as cr_booker
from app.bookers import spark as spark_booker
from app.bookers import wellnessliving as wl_booker

log = logging.getLogger(__name__)


async def _log_to_supabase(req: BookingRequest, resp: BookingResponse, loc_config: dict) -> None:
    """Insert a booking_requests row via Supabase REST API."""
    if not settings.supabase_url or not settings.supabase_service_key:
        log.warning("Supabase not configured, skipping booking log")
        return

    row = {
        "location_slug": req.location_slug,
        "crm": loc_config.get("crm", "unknown"),
        "store_id": loc_config.get("store_id"),
        "customer_name": req.customer_name,
        "customer_email": req.customer_email,
        "customer_phone": req.customer_phone,
        "requested_date": req.requested_date,
        "requested_time": req.requested_time,
        "instructor": req.instructor,
        "class_name": req.class_name,
        "source": req.source,
        "member_id": req.member_id,
        "lead_id": req.lead_id,
        "crm_booking_id": resp.booking_id,
        "crm_customer_id": resp.customer_id,
        "status": "confirmed" if resp.success else "failed",
        "error_message": "; ".join(resp.errors) if resp.errors else None,
        "response_payload": {
            "success": resp.success,
            "message": resp.message,
        },
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                f"{settings.supabase_url}/rest/v1/booking_requests",
                headers={
                    "apikey": settings.supabase_service_key,
                    "Authorization": f"Bearer {settings.supabase_service_key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                json=row,
            )
            if r.status_code not in (200, 201):
                log.warning("Supabase log failed: %d %s", r.status_code, r.text[:200])
            else:
                log.info("Booking logged to Supabase")
    except Exception as e:
        log.warning("Supabase log error: %s", e)


async def route_booking(req: BookingRequest) -> BookingResponse:
    """Route a booking request to the correct CRM or email fallback."""
    loc_config = LOCATION_CONFIG.get(req.location_slug)
    if not loc_config:
        return BookingResponse(
            success=False,
            message=f"Unknown location: {req.location_slug}",
            errors=[f"Location '{req.location_slug}' not found in config"],
        )

    crm = loc_config["crm"]
    log.info(
        "Routing booking: location=%s, crm=%s, customer=%s",
        req.location_slug, crm, req.customer_name,
    )

    if crm == "clubready":
        resp = await cr_booker.create_booking(req, loc_config)
    elif crm == "spark":
        resp = await spark_booker.create_booking(req, loc_config)
    elif crm == "wellnessliving":
        resp = await wl_booker.create_booking(req, loc_config)
    else:
        resp = BookingResponse(
            success=False,
            message=f"Unsupported CRM: {crm}",
            errors=[f"No booker for CRM type '{crm}'"],
        )

    # Log to Supabase (fire and forget, don't block response)
    try:
        await _log_to_supabase(req, resp, loc_config)
    except Exception as e:
        log.warning("Failed to log booking: %s", e)

    return resp


async def route_availability(req: AvailabilityRequest) -> AvailabilityResponse:
    """Get availability for a location. Currently only ClubReady supports this."""
    loc_config = LOCATION_CONFIG.get(req.location_slug)
    if not loc_config:
        return AvailabilityResponse(
            location_slug=req.location_slug,
            date=req.date,
            errors=[f"Unknown location: {req.location_slug}"],
        )

    crm = loc_config["crm"]

    if crm == "clubready":
        try:
            slots = await cr_booker.get_availability(
                store_id=loc_config["store_id"],
                username=loc_config["cr_username"],
                password=loc_config["cr_password"],
                date=req.date,
            )
            return AvailabilityResponse(
                location_slug=req.location_slug,
                date=req.date,
                slots=slots,
            )
        except Exception as e:
            return AvailabilityResponse(
                location_slug=req.location_slug,
                date=req.date,
                errors=[str(e)],
            )
    else:
        return AvailabilityResponse(
            location_slug=req.location_slug,
            date=req.date,
            errors=[f"Availability not supported for {crm}"],
        )
