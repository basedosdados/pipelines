"""CMS source column names -> Data Basis English snake_case names.

CMS names are English but long and inconsistently punctuated
(``Covered_Recipient_License_State_code1`` sits next to
``Covered_Recipient_Specialty_1``). The mapping below is explicit rather than
derived: a rule that shortens
``Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID`` correctly will
mangle something else, and the architecture keeps ``original_name`` so nothing
about the provenance is lost by renaming.

Identifier columns take the ``_id`` suffix, following ``us_cfpb_hmda``.
"""

import re

# Fragments that recur across the general, research and ownership files.
# Applied in order; the first matching rule wins.
SHARED = {
    "Change_Type": "change_type",
    "Covered_Recipient_Type": "covered_recipient_type",
    "Teaching_Hospital_CCN": "teaching_hospital_ccn",
    "Teaching_Hospital_ID": "teaching_hospital_id",
    "Teaching_Hospital_Name": "teaching_hospital_name",
    "Covered_Recipient_Profile_ID": "covered_recipient_profile_id",
    "Covered_Recipient_NPI": "covered_recipient_npi",
    "Covered_Recipient_First_Name": "covered_recipient_first_name",
    "Covered_Recipient_Middle_Name": "covered_recipient_middle_name",
    "Covered_Recipient_Last_Name": "covered_recipient_last_name",
    "Covered_Recipient_Name_Suffix": "covered_recipient_name_suffix",
    "Recipient_Primary_Business_Street_Address_Line1": "recipient_address_line_1",
    "Recipient_Primary_Business_Street_Address_Line2": "recipient_address_line_2",
    "Recipient_City": "recipient_city",
    "Recipient_State": "recipient_state",
    "Recipient_Zip_Code": "recipient_zip_code",
    "Recipient_Country": "recipient_country",
    "Recipient_Province": "recipient_province",
    "Recipient_Postal_Code": "recipient_postal_code",
    # Reporting entity: CMS calls it "applicable manufacturer or applicable
    # GPO"; the site and the profile file both call it the reporting entity.
    "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name": "submitting_entity_name",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID": "reporting_entity_id",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name": "reporting_entity_name",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State": "reporting_entity_state",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country": "reporting_entity_country",
    "Total_Amount_of_Payment_USDollars": "payment_amount_total",
    "Date_of_Payment": "payment_date",
    "Number_of_Payments_Included_in_Total_Amount": "payment_count",
    "Form_of_Payment_or_Transfer_of_Value": "payment_form",
    "Nature_of_Payment_or_Transfer_of_Value": "payment_nature",
    "City_of_Travel": "travel_city",
    "State_of_Travel": "travel_state",
    "Country_of_Travel": "travel_country",
    "Physician_Ownership_Indicator": "physician_ownership_indicator",
    "Third_Party_Payment_Recipient_Indicator": "third_party_payment_recipient_indicator",
    "Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value": "third_party_entity_name",
    "Charity_Indicator": "charity_indicator",
    "Third_Party_Equals_Covered_Recipient_Indicator": "third_party_equals_covered_recipient_indicator",
    "Contextual_Information": "contextual_information",
    "Delay_in_Publication_Indicator": "delay_in_publication_indicator",
    "Record_ID": "record_id",
    "Dispute_Status_for_Publication": "dispute_status",
    "Related_Product_Indicator": "related_product_indicator",
    "Program_Year": "year",
    "Payment_Publication_Date": "publication_date",
    "Noncovered_Recipient_Entity_Name": "noncovered_recipient_entity_name",
    "Preclinical_Research_Indicator": "preclinical_research_indicator",
    "Name_of_Study": "study_name",
    "ClinicalTrials_Gov_Identifier": "clinicaltrials_gov_id",
    "Research_Information_Link": "research_information_link",
    "Context_of_Research": "research_context",
    "Total_Amount_Invested_USDollars": "amount_invested_total",
    "Value_of_Interest": "interest_value",
    "Terms_of_Interest": "interest_terms",
    "Interest_Held_by_Physician_or_an_Immediate_Family_Member": "interest_held_by_physician_or_family",
    # Legacy (PY 2013-2015) spellings.
    "Physician_Profile_ID": "physician_profile_id",
    "Physician_NPI": "physician_npi",
    "Physician_First_Name": "physician_first_name",
    "Physician_Middle_Name": "physician_middle_name",
    "Physician_Last_Name": "physician_last_name",
    "Physician_Name_Suffix": "physician_name_suffix",
    "Physician_Primary_Type": "physician_primary_type",
    "Physician_Specialty": "physician_specialty",
    "Product_Indicator": "related_product_indicator",
}

# Numbered families. Each entry is (regex, replacement template using \1 for
# the trailing index).
NUMBERED = [
    (
        r"^Covered_Recipient_Primary_Type_(\d)$",
        r"covered_recipient_primary_type_\1",
    ),
    (r"^Covered_Recipient_Specialty_(\d)$", r"covered_recipient_specialty_\1"),
    (
        r"^Covered_Recipient_License_State_code(\d)$",
        r"covered_recipient_license_state_\1",
    ),
    (r"^Physician_License_State_code(\d)$", r"physician_license_state_\1"),
    # Modern product block: five slots of six fields each.
    (
        r"^Covered_or_Noncovered_Indicator_(\d)$",
        r"product_covered_indicator_\1",
    ),
    (
        r"^Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_(\d)$",
        r"product_type_\1",
    ),
    (r"^Product_Category_or_Therapeutic_Area_(\d)$", r"product_category_\1"),
    (
        r"^Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_(\d)$",
        r"product_name_\1",
    ),
    (r"^Associated_Drug_or_Biological_NDC_(\d)$", r"product_ndc_\1"),
    (r"^Associated_Device_or_Medical_Supply_PDI_(\d)$", r"product_pdi_\1"),
    # Legacy product columns: drugs and devices are listed separately, and
    # there is no covered/noncovered flag, category or device identifier.
    (r"^Name_of_Associated_Covered_Drug_or_Biological(\d)$", r"drug_name_\1"),
    (r"^NDC_of_Associated_Covered_Drug_or_Biological(\d)$", r"drug_ndc_\1"),
    (
        r"^Name_of_Associated_Covered_Device_or_Medical_Supply(\d)$",
        r"device_name_\1",
    ),
    (r"^Expenditure_Category(\d)$", r"expenditure_category_\1"),
]

# Principal investigator blocks, stripped of their slot index by
# ``pi_field`` below so the child table can carry one column per field.
PI_PREFIX = re.compile(r"^Principal_Investigator_(\d)_(.+)$")

PI_FIELDS = {
    "Covered_Recipient_Type": "covered_recipient_type",
    "Profile_ID": "covered_recipient_profile_id",
    "NPI": "covered_recipient_npi",
    "First_Name": "first_name",
    "Middle_Name": "middle_name",
    "Last_Name": "last_name",
    "Name_Suffix": "name_suffix",
    "Business_Street_Address_Line1": "address_line_1",
    "Business_Street_Address_Line2": "address_line_2",
    "City": "city",
    "State": "state",
    "Zip_Code": "zip_code",
    "Country": "country",
    "Province": "province",
    "Postal_Code": "postal_code",
    "Primary_Type": "primary_type_1",
    "Specialty": "specialty_1",
}

PI_NUMBERED = [
    (r"^Primary_Type_(\d)$", r"primary_type_\1"),
    (r"^Specialty_(\d)$", r"specialty_\1"),
    (r"^License_State_code(\d)$", r"license_state_\1"),
]


def split_principal_investigator(source: str) -> tuple[int, str] | None:
    """Return ``(slot, field_name)`` for a principal-investigator column.

    ``None`` for every other column. The returned field name is the name the
    column takes in the ``*_principal_investigator`` child table, where the
    slot itself becomes the ``principal_investigator_number`` column.
    """
    m = PI_PREFIX.match(source)
    if not m:
        return None
    slot, rest = int(m.group(1)), m.group(2)
    if rest in PI_FIELDS:
        return slot, PI_FIELDS[rest]
    for pattern, repl in PI_NUMBERED:
        if re.match(pattern, rest):
            return slot, re.sub(pattern, repl, rest)
    raise KeyError(f"unmapped principal investigator field: {source}")


def rename(source: str) -> str:
    """Data Basis column name for a CMS detail-file column."""
    if source in SHARED:
        return SHARED[source]
    for pattern, repl in NUMBERED:
        if re.match(pattern, source):
            return re.sub(pattern, repl, source)
    raise KeyError(f"unmapped column: {source}")


# --- Entity (profile) files -------------------------------------------------
# Each profile table drops the prefix that merely restates its own name, so
# ``Covered_Recipient_Profile_City`` becomes ``city``. Keys that identify the
# row keep their full name.
PROFILE = {
    "covered_recipient_profile": {
        "Covered_Recipient_Profile_ID": "covered_recipient_profile_id",
        "Covered_Recipient_NPI": "covered_recipient_npi",
        "Covered_Recipient_Profile_Type": "profile_type",
        "Associated_Covered_Recipient_Profile_ID_1": "associated_profile_id_1",
        "Associated_Covered_Recipient_Profile_ID_2": "associated_profile_id_2",
        "Covered_Recipient_Profile_First_Name": "first_name",
        "Covered_Recipient_Profile_Middle_Name": "middle_name",
        "Covered_Recipient_Profile_Last_Name": "last_name",
        "Covered_Recipient_Profile_Suffix": "name_suffix",
        "Covered_Recipient_Profile_Alternate_First_Name": "alternate_first_name",
        "Covered_Recipient_Profile_Alternate_Middle_Name": "alternate_middle_name",
        "Covered_Recipient_Profile_Alternate_Last_Name": "alternate_last_name",
        "Covered_Recipient_Profile_Alternate_Suffix": "alternate_name_suffix",
        "Covered_Recipient_Profile_Address_Line_1": "address_line_1",
        "Covered_Recipient_Profile_Address_Line_2": "address_line_2",
        "Covered_Recipient_Profile_City": "city",
        "Covered_Recipient_Profile_State": "state",
        "Covered_Recipient_Profile_Zipcode": "zip_code",
        "Covered_Recipient_Profile_Country_Name": "country",
        "Covered_Recipient_Profile_Province_Name": "province",
        "Covered_Recipient_Profile_Primary_Specialty": "primary_specialty",
        **{
            f"Covered_Recipient_Profile_OPS_Taxonomy_{i}": f"taxonomy_{i}"
            for i in range(1, 7)
        },
        **{
            f"Covered_Recipient_Profile_License_State_Code_{i}": f"license_state_{i}"
            for i in range(1, 6)
        },
    },
    "teaching_hospital_profile": {
        "Teaching_Hospital_CCN": "teaching_hospital_ccn",
        "Teaching_Hospital_NAME": "name",
        "Teaching_Hospital_ADDRESS1": "address_line_1",
        "Teaching_Hospital_ADDRESS2": "address_line_2",
        "Teaching_Hospital_CITY": "city",
        "Teaching_Hospital_STATE": "state",
        "Teaching_Hospital_ZIP_CODE": "zip_code",
        **{
            f"Teaching_Hospital_Alternate_Name{i}": f"alternate_name_{i}"
            for i in range(1, 6)
        },
    },
    "reporting_entity_profile": {
        "AMGPO_Making_Payment_ID": "reporting_entity_id",
        "AMGPO_Making_Payment_Name": "name",
        "AMGPO_Making_Payment_State": "state",
        "AMGPO_Making_Payment_Country": "country",
        **{
            f"AMGPO_Making_Payment_Alternate_Name{i}": f"alternate_name_{i}"
            for i in range(1, 6)
        },
    },
    "provider_profile_mapping": {
        "Primary_Provider_Profile_ID": "primary_profile_id",
        "Secondary_Provider_Profile_ID": "secondary_profile_id",
    },
}

# --- Summary reports --------------------------------------------------------
# Names shared across the summary family. Per-table overrides follow.
SUMMARY_SHARED = {
    "Program_Year": "year",
    "Recipient_ID": "recipient_id",
    "Recipient_Type": "recipient_type",
    "Recipient_Name": "recipient_name",
    "Covered_Recipient_NPI": "covered_recipient_npi",
    "Covered_Recipient_Profile_ID": "covered_recipient_profile_id",
    "Covered_Recipient_Profile_Type": "profile_type",
    "Covered_Recipient_Profile_First_Name": "first_name",
    "Covered_Recipient_Profile_Middle_Name": "middle_name",
    "Covered_Recipient_Profile_Last_Name": "last_name",
    "Covered_Recipient_Profile_Suffix": "name_suffix",
    "Covered_Recipient_Profile_Alternate_First_Name": "alternate_first_name",
    "Covered_Recipient_Profile_Alternate_Middle_Name": "alternate_middle_name",
    "Covered_Recipient_Profile_Alternate_Last_Name": "alternate_last_name",
    "Covered_Recipient_Profile_Alternate_Suffix": "alternate_name_suffix",
    "Covered_Recipient_Profile_Address_Line_1": "address_line_1",
    "Covered_Recipient_Profile_Address_Line_2": "address_line_2",
    "Covered_Recipient_Profile_City": "city",
    "Covered_Recipient_Profile_State": "state",
    "Covered_Recipient_Profile_Zipcode": "zip_code",
    "Covered_Recipient_Profile_Country_Name": "country",
    "Covered_Recipient_Profile_Province_Name": "province",
    "Covered_Recipient_Profile_Primary_Specialty": "primary_specialty",
    **{
        f"Covered_Recipient_Profile_License_State_Code_{i}": f"license_state_{i}"
        for i in range(1, 6)
    },
    "Teaching_Hospital_Name": "teaching_hospital_name",
    "Teaching_Hospital_CCN": "teaching_hospital_ccn",
    "Teaching_Hospital_NAME": "teaching_hospital_name",
    "Teaching_Hospital_ADDRESS1": "address_line_1",
    "Teaching_Hospital_ADDRESS2": "address_line_2",
    "Teaching_Hospital_CITY": "city",
    "Teaching_Hospital_STATE": "state",
    "Teaching_Hospital_ZIP_CODE": "zip_code",
    "AMGPO_ID": "reporting_entity_id",
    "AMGPO_Name": "reporting_entity_name",
    "AMGPO_Making_Payment_ID": "reporting_entity_id",
    "AMGPO_Making_Payment_Name": "reporting_entity_name",
    "AMGPO_Making_Payment_State": "reporting_entity_state",
    "AMGPO_Making_Payment_Country": "reporting_entity_country",
    "Nature_Of_Payment_Type_Code": "payment_nature_code",
    "Nature_of_Payment": "payment_nature",
    "Payment_Type": "payment_type",
    "Metric_Level": "metric_level",
    "Number_of_Transaction": "transaction_count",
    "Total_Amount": "amount_total",
    "Country_Code": "country_code",
    "Country_Name": "country_name",
    "State_Code": "state_code",
    "State_Name": "state_name",
    "Taxonomy_Code": "taxonomy_code",
    "Provider_Type_Description": "provider_type_description",
    "Classification": "classification",
    "Specialization": "specialization",
    "Total_Number_of_Physicians": "physician_count",
    "Total_Number_of_Non_Physician_Practitioners": "non_physician_practitioner_count",
    "Total_Number_of_Teaching_Hospitals": "teaching_hospital_count",
    "Total_Payment_Amount_Physician": "payment_amount_total_physician",
    "Total_Payment_Amount_Non_Physician_Practitioner": "payment_amount_total_non_physician_practitioner",
    "Total_Payment_Amount_Teaching_Hospital": "payment_amount_total_teaching_hospital",
    "Mean_Total_Payment_Amount_Physician": "payment_amount_mean_physician",
    "Mean_Total_Payment_Amount_Non_Physician_Practitioner": "payment_amount_mean_non_physician_practitioner",
    "Mean_Total_Payment_Amount_Teaching_Hospital": "payment_amount_mean_teaching_hospital",
    "Median_Total_Payment_Amount_Physician": "payment_amount_median_physician",
    "Median_Total_Payment_Amount_Non_Physician_Practitioner": "payment_amount_median_non_physician_practitioner",
    "Median_Total_Payment_Amount_Teaching_Hospital": "payment_amount_median_teaching_hospital",
    "Total_Payment_Count_Physician": "payment_count_total_physician",
    "Total_Payment_Count_Non_Physician_Practitioner": "payment_count_total_non_physician_practitioner",
    "Total_Payment_Count_Teaching_Hospital": "payment_count_total_teaching_hospital",
    "Mean_Total_Payment_Count_Physician": "payment_count_mean_physician",
    "Mean_Total_Payment_Count_Non_Physician_Practitioner": "payment_count_mean_non_physician_practitioner",
    "Mean_Total_Payment_Count_Teaching_Hospital": "payment_count_mean_teaching_hospital",
    "Median_Total_Payment_Count_Physician": "payment_count_median_physician",
    "Median_Total_Payment_Count_Non_Physician_Practitioner": "payment_count_median_non_physician_practitioner",
    "Median_Total_Payment_Count_Teaching_Hospital": "payment_count_median_teaching_hospital",
    "General_Total_Payment": "general_payment_amount_total",
    "Research_Total_Payment": "research_payment_amount_total",
    "Invested_Total_Amount": "ownership_amount_invested_total",
    "Interest_Total_Amount": "ownership_interest_value_total",
    "General_Total_Transactions": "general_transaction_count",
    "Research_Total_Transactions": "research_transaction_count",
    "Invested_Total_Transactions": "ownership_invested_transaction_count",
    "Interest_Total_Transactions": "ownership_interest_transaction_count",
    "Total_Associated_Research_Payments": "associated_research_payment_amount_total",
    "Total_Associated_Research_Transactions": "associated_research_transaction_count",
    "Total_Disputed_Transactions": "disputed_transaction_count",
    "Total_Undisputed_Transactions": "undisputed_transaction_count",
    "Has_Multiple_IDs": "has_multiple_ids",
    "Total_Amount_General": "general_payment_amount_total",
    "Total_Amount_Research": "research_payment_amount_total",
    "Total_Amount_Investment": "ownership_amount_invested_total",
    "Total_Amount_Interest": "ownership_interest_value_total",
    "Trans_General": "general_transaction_count",
    "Trans_General_Physician": "general_transaction_count_physician",
    "Trans_General_Non_Physician_Practitioner": "general_transaction_count_non_physician_practitioner",
    "Trans_General_Teachinghospital": "general_transaction_count_teaching_hospital",
    "Trans_Research": "research_transaction_count",
    "Trans_Research_Physician": "research_transaction_count_physician",
    "Trans_Research_Non_Physician_Practitioner": "research_transaction_count_non_physician_practitioner",
    "Trans_Research_Teachinghospital": "research_transaction_count_teaching_hospital",
    "Trans_Invested": "ownership_invested_transaction_count",
    "Dashboard_Row_Number": "dashboard_row_number",
    "Data_Metrics": "metric",
}


def rename_profile(table: str, source: str) -> str:
    return PROFILE[table][source]


def rename_summary(source: str) -> str:
    return SUMMARY_SHARED[source]
