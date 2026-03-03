"""Pydantic models for the universal booking system."""

from __future__ import annotations

from pydantic import BaseModel


class BookingRequest(BaseModel):
    """Incoming booking request from frontend (via n8n webhook)."""

    location_slug: str  # e.g. "sz-westborough", "ima-worcester"
    customer_name: str
    customer_email: str | None = None
    customer_phone: str | None = None
    requested_date: str  # ISO date: "2026-03-10"
    requested_time: str  # "14:00" or "2:00 PM"
    instructor: str | None = None
    class_name: str | None = None
    member_id: str | None = None   # CRM member/customer ID (e.g. ClubReady userId)
    lead_id: str | None = None     # CRM lead ID
    source: str = "booking_page"  # "booking_page" or "nft_page"
    notes: str | None = None


class BookingResponse(BaseModel):
    """Response back to frontend."""

    success: bool
    booking_id: str | None = None  # CRM booking ID if created
    customer_id: str | None = None  # CRM customer ID
    message: str = ""
    errors: list[str] = []


class AvailabilityRequest(BaseModel):
    """Request available time slots for a location."""

    location_slug: str
    date: str  # ISO date: "2026-03-10"
    instructor_id: str | None = None
    service_id: str | None = None


class AvailabilitySlot(BaseModel):
    """A single available time slot."""

    start_time: str  # ISO datetime or HH:MM
    end_time: str
    instructor_name: str | None = None
    instructor_id: str | None = None
    service_name: str | None = None
    service_id: str | None = None
    spots_available: int | None = None


class AvailabilityResponse(BaseModel):
    """Available slots for a given date/location."""

    location_slug: str
    date: str
    slots: list[AvailabilitySlot] = []
    errors: list[str] = []


# Location -> CRM mapping
LOCATION_CONFIG: dict[str, dict] = {
    "sz-westborough": {
        "crm": "clubready",
        "store_id": "15077",
        "cr_username": "BillSWQE",
        "cr_password": "VJYiAB7fvqUv6T",
        "name": "StretchZone Westborough",
    },
    "sz-west-boylston": {
        "crm": "clubready",
        "store_id": "14803",
        "cr_username": "BillSWQE",
        "cr_password": "VJYiAB7fvqUv6T",
        "name": "StretchZone West Boylston",
    },
    "stretchlab-carlsbad": {
        "crm": "clubready",
        "store_id": "12727",
        "cr_username": "bill@velocityaipartners.ai",
        "cr_password": "ub4LdI1J5Dcg3M",
        "name": "StretchLab Carlsbad",
    },
    "sz-dfw": {
        "crm": "unsupported",
        "name": "StretchZone DFW",
    },
    "sz-baton-rouge": {
        "crm": "unsupported",
        "name": "StretchZone Baton Rouge",
    },
    "ima-westborough": {
        "crm": "spark",
        "name": "IMA Westborough",
    },
    "ima-worcester": {
        "crm": "wellnessliving",
        "name": "IMA Worcester",
    },
}
