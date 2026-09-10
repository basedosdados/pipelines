"""Constants for the au_doe_higher_education recurring pipeline."""

from enum import Enum


class constants(Enum):
    """Source locations, table list and partitioning for the annual refresh."""

    DATASET_ID = "au_doe_higher_education"
    DIRECTORY_DATASET_ID = "br_bd_diretorios_au"
    DIRECTORY_TABLE_ID = "higher_education_institution"

    BASE_URL = "https://www.education.gov.au"

    #: The three collection landing pages. Student and undergraduate resources
    #: are linked from the landing page itself; staff data is one level deeper,
    #: under a per-year sub-page (``selected-...-2025-staff-data``).
    STUDENT_PAGE = "/higher-education-statistics/student-data"
    STAFF_PAGE = "/higher-education-statistics/staff-data"
    UAO_PAGE = (
        "/higher-education-statistics/"
        "undergraduate-applications-offers-and-acceptances-publications"
    )

    #: The site sits behind Akamai with a Signal Sciences WAF
    #: (``x-lagoon: ...sigsci-ingress-nginx...``). A hand-rolled User-Agent is
    #: not enough: from the Kubernetes pool the WAF stalls a plain ``requests``
    #: client rather than refusing it, so the request never returns at all.
    #: curl_cffi reproduces Chrome's TLS and HTTP/2 fingerprint and sends its
    #: own browser headers, which is why none are set here — overriding them
    #: would contradict the fingerprint being impersonated.
    IMPERSONATE = "chrome"

    #: ``(connect, read)`` seconds. Deliberately modest. A stalled read here is
    #: a block, not slowness: the 2026-09-04 run already carried a 300s read
    #: timeout and merely spent 2h13m hitting it, so a longer leash only delays
    #: the failure and risks the pod being evicted before it is ever reported.
    REQUEST_TIMEOUT = (30, 120)

    #: Bounded retry budget per URL: three attempts with 5s then 10s of
    #: backoff, so an unreachable host costs about six minutes and then raises.
    RETRY_ATTEMPTS = 3
    RETRY_BACKOFF_SECONDS = 5

    #: Local filename -> regex matching that document's resource slug. The year
    #: is group 1 and the newest match wins, so a renamed release is picked up
    #: without editing this table. The undergraduate appendices are matched
    #: loosely because the department switched from a single year
    #: (``...-2021-appendix``) to a range (``...-20222024-appendices``).
    RESOURCES = {
        "enrol": r"^perturbed-student-enrolments-pivot-table-(\d{4})$",
        "load": r"^perturbed-student-load-pivot-table-(\d{4})$",
        "compl": r"^perturbed-award-course-completions-pivot-table-(\d{4})$",
        "staff": r"^(\d{4})-staff-pivot-table$",
        "sec11_equity": r"^(\d{4})-section-11-equity-groups$",
        "sec15_attrition": r"^(\d{4})-section-15-attrition-success-and-retention$",
        "sec16_equityperf": r"^(\d{4})-section-16-equity-performance-data$",
        "sec17_complrate": r"^(\d{4})-section-17-completion-rates$",
        # The department moved from a single year ("...-2021-appendix") to a
        # range ("...-20222024-appendices"), so the year is the last four
        # digits before the suffix. The 2021 file is fetched as well and is
        # not interchangeable: acceptances were discontinued after that
        # round and are read only from it.
        "uao_current": r"^undergraduate-applications.*?(\d{4})-appendi[cx]",
        "uao_2021": r"^undergraduate-applications-offers-and-acceptances-(2021)-appendix$",
    }

    #: Tables built by the flow, in dependency order. The directory is
    #: materialised first because every other model's relationship test
    #: resolves against it.
    TABLES = [
        "student_enrolment",
        "student_load",
        "award_course_completion",
        "staff",
        "student_equity_group",
        "student_equity_performance",
        "equity_reference_value",
        "student_attrition_retention_success",
        "student_completion_rate",
        "application_offer",
    ]

    #: Tables whose partition column is not ``year``.
    PARTITION_OVERRIDE = {"student_completion_rate": "cohort_start_year"}

    #: The tables whose coverage the source publishes as a rolling window. A
    #: release carries only the last five to seven years, so a refresh must
    #: replace exactly the partitions it rebuilds and leave older ones alone.
    ROLLING_WINDOW_TABLES = [
        "student_enrolment",
        "student_load",
        "award_course_completion",
        "staff",
    ]
