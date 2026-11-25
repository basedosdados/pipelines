"""Hand-written code legends, for columns OpenFEMA does not enumerate cleanly.

Everything else is parsed straight out of the cached field dictionary by
``build_dicionario.py``. An entry here overrides the parser, so add one only
when the parse is wrong or the source lists no values at all — and say why.
"""

from __future__ import annotations

# Used wherever the data carries a code that appears in no FEMA legend. The
# value is kept and labelled honestly rather than guessed at or dropped.
UNDOC = "Not documented by FEMA; reported value retained as published"


def _flood_zones() -> dict[str, str]:
    """FIRM flood zones.

    OpenFEMA describes these as zone *families* ("AE, A1-A30 - Special Flood
    with Base Flood Elevation on FIRM") rather than as an enumeration, while
    the data carries each numbered zone separately (A01…A30, V01…V30). The
    families are expanded here so every value in the data has a label.
    """
    zones = {
        "A": "Special flood hazard area, no base flood elevation on the FIRM",
        "AE": "Special flood hazard area with a base flood elevation on the FIRM",
        "A99": (
            "Special flood hazard area protected by a flood control system "
            "under construction"
        ),
        "AH": "Special flood hazard area subject to shallow ponding",
        "AHB": (
            "Special flood hazard area subject to shallow ponding, with a base "
            "flood elevation"
        ),
        "AO": "Special flood hazard area subject to sheet flow",
        "AOB": (
            "Special flood hazard area subject to sheet flow, with a base "
            "flood elevation"
        ),
        "AR": (
            "Special flood hazard area with temporarily increased risk from a "
            "decertified flood protection system"
        ),
        "B": (
            "Moderate flood hazard from the primary water source; superseded "
            "by zone X in 1986"
        ),
        "C": (
            "Minimal flood hazard from the primary water source; superseded by "
            "zone X in 1986"
        ),
        "D": "Possible but undetermined flood hazard, not analysed",
        "V": "Coastal high hazard area, no base flood elevation on the FIRM",
        "VE": "Coastal high hazard area with a base flood elevation on the FIRM",
        "X": "Moderate to minimal flood hazard, outside the special flood hazard area",
        # Present in the data but not in FEMA's published zone list. Labelled
        # as undocumented rather than guessed at.
        "AA": "Not a documented FIRM zone; reported value retained as published",
        "AS": "Not a documented FIRM zone; reported value retained as published",
    }
    for n in range(1, 31):
        zones[f"A{n:02d}"] = (
            f"Special flood hazard area with a base flood elevation, "
            f"numbered zone A{n}"
        )
        zones[f"V{n:02d}"] = (
            f"Coastal high hazard area with a base flood elevation, "
            f"numbered zone V{n}"
        )
    return zones


LEGENDS: dict[str, dict[str, str]] = {
    # FEMA's description links to a web page instead of listing the codes. The
    # labels are taken from the paired damageCategoryDescrip in the data.
    "damage_category_code": {
        "A": "Debris removal",
        "B": "Emergency protective measures",
        "C": "Roads and bridges",
        "D": "Water control facilities",
        "E": "Buildings and equipment",
        "F": "Utilities",
        "G": "Parks, recreational facilities and other items",
        "I": "Building code management and enforcement",
        "Z": "Management and direct administrative costs",
    },
    # FEMA's description links to the declaration-process page.
    "declaration_type": {
        "DR": "Major disaster declaration",
        "EM": "Emergency declaration",
        "FM": "Fire management assistance declaration",
    },
    "rated_flood_zone": _flood_zones(),
    "flood_zone_current": _flood_zones(),
    # The source states the premium credit per class in prose. Summarised as
    # the discount each class earns inside the special flood hazard area.
    "crs_class_code": {
        "1": "CRS class 1, 45% premium discount inside the special flood hazard area",
        "2": "CRS class 2, 40% premium discount inside the special flood hazard area",
        "3": "CRS class 3, 35% premium discount inside the special flood hazard area",
        "4": "CRS class 4, 30% premium discount inside the special flood hazard area",
        "5": "CRS class 5, 25% premium discount inside the special flood hazard area",
        "6": "CRS class 6, 20% premium discount inside the special flood hazard area",
        "7": "CRS class 7, 15% premium discount inside the special flood hazard area",
        "8": "CRS class 8, 10% premium discount inside the special flood hazard area",
        "9": "CRS class 9, 5% premium discount inside the special flood hazard area",
        "10": "CRS class 10, no premium discount",
    },
    # The source labels run to a paragraph each; condensed to the distinction
    # that matters, which is what was certified and how the policy was rated.
    "elevation_certificate_indicator": {
        "1": (
            "No elevation certificate; effective before 1 October 1982, rated "
            "at no-base-flood-elevation +2 to +4 feet"
        ),
        "2": (
            "No elevation certificate; effective on or after 1 October 1982, "
            "rated at no-elevation-certificate rates"
        ),
        "3": (
            "Elevation certificate with a base flood elevation, rated with "
            "base flood elevation"
        ),
        "4": (
            "Elevation certificate without a base flood elevation, rated at "
            "no-base-flood-elevation rates"
        ),
        "A": "Basement or subgrade crawlspace",
        "B": "Fill or crawlspace",
        "C": "Piles, piers or columns with enclosure",
        "D": "Piles, piers or columns without enclosure",
        "E": "Slab on grade",
    },
    # Two 1-digit and 2-digit code families in one column; the prose separators
    # defeat the generic parser.
    "occupancy_type": {
        "1": "Single family residence",
        "2": "Residential building with 2 to 4 units",
        "3": "Residential building with more than 4 units",
        "4": "Non-residential building",
        "6": "Non-residential business",
        "11": (
            "Risk Rating 2.0: single-family residential building, excluding a "
            "mobile home or a unit within a multi-unit building"
        ),
        "12": (
            "Risk Rating 2.0: residential non-condominium building with 2 to 4 "
            "units, all units insured"
        ),
        "13": (
            "Risk Rating 2.0: residential non-condominium building with 5 or "
            "more units, all units insured"
        ),
        "14": "Risk Rating 2.0: residential mobile or manufactured home",
        "15": (
            "Risk Rating 2.0: residential condominium association covering a "
            "building with one or more units"
        ),
        "16": (
            "Risk Rating 2.0: single residential unit within a multi-unit "
            "building"
        ),
        "17": "Risk Rating 2.0: non-residential mobile or manufactured home",
        "18": "Risk Rating 2.0: non-residential building",
        "19": (
            "Risk Rating 2.0: non-residential unit within a multi-unit building"
        ),
    },
    # FEMA lists only 1, 3 and 9. The policy file also carries 0, 2, 4, 5 and
    # 6, which appear in no published list — labelled as undocumented rather
    # than inferred from the code being a plausible number of years.
    "policy_term_indicator": {
        "1": "One year",
        "3": "Three years, for policies effective before 1 May 1999",
        "9": "Other term, between one and three years",
        "0": UNDOC,
        "2": UNDOC,
        "4": UNDOC,
        "5": UNDOC,
        "6": UNDOC,
    },
    "basement_enclosure_crawlspace_type": {
        "0": "None",
        "1": "Finished basement or enclosure",
        "2": "Unfinished basement or enclosure",
        "3": "Crawlspace",
        "4": "Subgrade crawlspace",
    },
    "number_of_floors_in_insured_building": {
        "1": "One floor",
        "2": "Two floors",
        "3": "Three or more floors",
        "4": "Split-level",
        "5": "Manufactured or mobile home, or travel trailer on a foundation",
        "6": (
            "Townhouse or rowhouse with three or more floors, RCBAP low-rise "
            "only"
        ),
    },
    "foundation_type": {
        "0": "Not reported",
        "1": "Slab",
        "2": "Basement",
        "3": "Crawlspace",
        "4": "Elevated without enclosure, on posts, piles or piers",
        "5": "Elevated with enclosure, on posts, piles or piers",
        "6": "Elevated with enclosure, not on posts, piles or piers",
    },
    "location_of_contents": {
        "1": "Basement, enclosure, crawlspace or subgrade crawlspace only",
        "2": "Basement, enclosure, crawlspace or subgrade crawlspace and above",
        "3": "Lowest floor only, above ground level, no basement or enclosure",
        "4": (
            "Lowest floor above ground level and higher floors, no basement or "
            "enclosure"
        ),
        "5": "Above ground level, more than one full floor",
        "6": (
            "Manufactured or mobile home, or travel trailer on a foundation"
        ),
        "7": "Enclosure or crawlspace and above",
    },
    "building_purpose": {
        "R": "Residential, 100%",
        "N": "Non-residential, 100%",
        "M": "Mixed use",
    },
    "building_over_water_type": {
        "1": "Not over water",
        "2": "Partially over water",
        "3": "Fully or entirely over water",
    },
    # The prose separators (no ';' between entries) defeat the generic parser,
    # and the data carries codes 91/92/95 that a partial parse missed.
    "obstruction_type": {
        "10": "Free of obstruction",
        "15": (
            "With obstruction: enclosure or crawlspace with proper openings, "
            "not used for rating; not applicable in V zones"
        ),
        "20": (
            "With obstruction: under 300 sq ft with breakaway walls, no "
            "machinery below the lowest elevated floor, or machinery at or "
            "above the base flood elevation"
        ),
        "24": (
            "With obstruction: under 300 sq ft with breakaway walls or "
            "finished enclosure, machinery below the base flood elevation"
        ),
        "30": (
            "With obstruction: 300 sq ft or more with breakaway walls, no "
            "machinery below the base flood elevation"
        ),
        "34": (
            "With obstruction: 300 sq ft or more with breakaway walls or "
            "finished enclosure, machinery below the base flood elevation"
        ),
        "40": (
            "With obstruction: no walls, machinery below the base flood "
            "elevation"
        ),
        "50": (
            "With obstruction: non-breakaway walls, crawlspace or finished "
            "enclosure, no machinery below the lowest elevated floor"
        ),
        "54": (
            "With obstruction: non-breakaway walls, crawlspace or finished "
            "enclosure, machinery below the lowest elevated floor"
        ),
        "60": "With obstruction",
        "70": "With certification, subgrade crawlspace in an A zone",
        "80": "Without certification, subgrade crawlspace, all zones",
        "90": (
            "With enclosure: elevated building with an elevator below the base "
            "flood elevation in an A zone, no other enclosure"
        ),
        "91": (
            "Free of obstruction: elevated building in a V zone with lattice, "
            "slats or shutters enclosing the elevator below the base flood "
            "elevation"
        ),
        "92": (
            "With enclosure: elevated building with an elevator below the base "
            "flood elevation in an A zone, enclosure without proper openings"
        ),
        "94": (
            "With obstruction: elevated building with an elevator below the "
            "base flood elevation in a V zone, no other obstruction"
        ),
        "95": (
            "With obstruction: elevated building in a V zone with an "
            "unfinished breakaway wall obstruction and elevator below the base "
            "flood elevation, no machinery below it"
        ),
        "96": (
            "With obstruction: elevated building in a V zone with a breakaway "
            "wall obstruction and an elevator below the base flood elevation"
        ),
        "97": (
            "With obstruction: elevated building in a V zone with an elevator "
            "and building machinery below the base flood elevation, no other "
            "obstruction"
        ),
        "98": (
            "With obstruction: elevated building in a V zone with a breakaway "
            "wall obstruction, an elevator and building machinery below the "
            "base flood elevation"
        ),
    },
    # FEMA documents 0-4 and 7-9 plus A-D. The data also carries 5, 6, @, N and
    # Z, which appear in no published list; more than one cause can be recorded
    # per claim, so combined values occur.
    "cause_of_damage": {
        "0": "Other causes",
        "1": "Tidal water overflow",
        "2": "Stream, river or lake overflow",
        "3": "Alluvial fan overflow",
        "4": "Accumulation of rainfall or snowmelt",
        "7": "Erosion, demolition; only for a loss before 23 September 1995",
        "8": "Erosion, removal; only for a loss before 23 September 1995",
        "9": "Earth movement: landslide, land subsidence or sinkhole",
        "A": "Closed basin lake",
        "B": "Expedited claim handling without site inspection",
        "C": "Expedited claim handling with follow-up site inspection",
        "D": "Expedited claim handling by remote adjustment",
        "5": UNDOC,
        "6": UNDOC,
        "@": UNDOC,
        "N": UNDOC,
        "Z": UNDOC,
    },
    "disaster_assistance_coverage_required": {
        "0": "Not required",
        "1": "Small Business Administration",
        "2": "Federal Emergency Management Agency",
        "3": UNDOC,
        "4": "Department of Health and Human Services; cancelled 1 October 2009",
        "5": "Other agency",
    },
    # FEMA documents these zero-padded (01-20) but the data stores them
    # unpadded, so the legend is keyed the way the data is.
    "building_description_code": {
        "1": "Main house",
        "2": "Detached guest house",
        "3": "Detached garage",
        "4": "Agricultural building",
        "5": "Warehouse",
        "6": "Pool house, clubhouse or recreation building",
        "7": "Tool or storage shed",
        "8": "Other",
        "9": "Barn",
        "10": "Apartment building",
        "11": "Apartment unit",
        "12": "Cooperative building",
        "13": "Cooperative unit",
        "14": "Commercial building",
        "15": "Condominium, entire building",
        "16": "Condominium unit",
        "17": "House of worship",
        "18": "Manufactured or mobile home",
        "19": "Travel trailer",
        "20": "Townhouse or rowhouse",
        # 21, 32 and 34 occur in the data but in no published FEMA list.
        "21": UNDOC,
        "32": UNDOC,
        "34": UNDOC,
    },
    # The data abbreviates FEMA's "RatingEngine" to "RE", and the trailing
    # entry defeats the parser.
    "rate_method": {
        "1": "Manual",
        "2": "Specific",
        "3": "Alternative",
        "4": "V-zone risk factor rating form",
        "5": "Underinsured condominium master policy",
        "6": "Provisional",
        "7": "Preferred Risk Policy, outside the special flood hazard area",
        "8": "Tentative",
        "9": "Mortgage Portfolio Protection Program policy",
        "A": "Optional post-1981 V zone",
        "B": "Pre-FIRM policy with elevation rating, manual rate tables",
        "E": "FEMA pre-FIRM special rates",
        "F": "Leased federal property",
        "G": "Group Flood Insurance Policy",
        "I": "Incomplete data, provisional rating at renewal",
        "P": "Preferred Risk Policy eligibility extension, first renewal year",
        "Q": "Preferred Risk Policy eligibility extension, later renewals",
        "R": "Newly mapped into the special flood hazard area",
        "S": "FEMA special rates",
        "T": "Severe repetitive loss property; invalid from 1 October 2013",
        "W": "Pre-FIRM policy with elevation rating, submit-for-rate",
        "RE": "Risk Rating 2.0, rates calculated by FEMA from risk factors",
    },
    "regular_emergency_program_indicator": {
        "R": "Regular Program",
        "E": "Emergency Program",
    },
}
