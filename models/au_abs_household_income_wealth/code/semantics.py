"""How each ABS 6523.0 cube table maps onto the long fact table.

The cubes put the survey year, the geography, the population breakdown and the
measured quantity on whichever axis suited the page. Four arrangements occur,
and one cube mixes them across its own sheets, so the axis roles are declared
per table here rather than guessed at run time. Everything else — the labels,
the units, the values — is read from the workbook.

``breakdown`` says where the population split lives:

``columns``   the column headers are the split; the rows are the measure.
``row_leaf``  the rows read "measure > category", as in table 1.1's "Mean
              income per week > Lowest quintile"; the columns are survey years.
              Only a leaf in ``BREAKDOWN_VALUES`` is taken as a category, which
              leaves rows such as "Gini coefficient" as measures of the whole
              population.
``row_top``   the rows read "category type > category", as in table 3.3's "Age
              of household reference person > 15–24"; the measure is on the
              columns. The type is looked up in ``ROW_TOP_TYPES``.
``none``      there is no split; every estimate is for all households.

``geography`` is ``columns`` where the column headers are places (cube 13), and
otherwise the one place the table covers.
"""

# The release these axis roles were read from. ABS renumbers the cubes between
# releases: the 2017-18 release inserts an "INCOME, GOVERNMENT BENEFITS AND
# TAXES" sheet into cubes 1 and 5 to 13, which shifts tables 13.5 to 13.12 by
# one, so 13.5 is equivalised disposable income for a state in 2019-20 and
# income, benefits and taxes in 2017-18. A table id therefore does not identify
# the same table across releases, and running this config against another one
# would mislabel the geography level of eight tables without failing. Adding a
# release means auditing its own table map, not pointing the script at another
# directory.
AUDITED_RELEASE = "2019-20"

AUSTRALIA = "Australia"

# Row leaves that name a position in a distribution rather than a measure.
BREAKDOWN_VALUES = {
    "Lowest quintile",
    "Second quintile",
    "Third quintile",
    "Fourth quintile",
    "Highest quintile",
    "Adjusted lowest income quintile",
    "All households",
    "All persons",
}

# Row-top labels that name a classification, for ``row_top`` tables.
ROW_TOP_TYPES = {
    "Age": "age_of_person",
    "Age of household reference person": "age_of_reference_person",
    "Contribution of government pensions and allowances to gross household"
    " income": "contribution_of_government_pensions_to_gross_income",
    "Equivalised disposable household income per week": (
        "equivalised_disposable_household_income_range"
    ),
    "Equivalised disposable household income quintile": (
        "equivalised_disposable_household_income_quintile"
    ),
    "Family composition of household": "household_composition",
    "Gross household income per week": "gross_household_income_range",
    "Labour force status": "labour_force_status",
    "Main source of household income": "main_source_of_household_income",
    "Net worth quintile": "net_worth_quintile",
    "Persons living in": "area_of_usual_residence",
    "Net worth range": "net_worth_range",
    "States and territories": "state_or_territory",
    "Tenure and landlord type": "tenure_and_landlord_type",
}

EDHI_QUINTILE = "equivalised_disposable_household_income_quintile"
GROSS_QUINTILE = "gross_household_income_quintile"
NET_WORTH_QUINTILE = "net_worth_quintile"

# table id -> (breakdown mode, breakdown type, geography)
TABLES: dict[str, tuple[str, str, str]] = {
    "1.1": ("row_leaf", EDHI_QUINTILE, AUSTRALIA),
    "1.2": ("row_leaf", GROSS_QUINTILE, AUSTRALIA),
    "1.3": ("row_top", "", AUSTRALIA),
    "1.4": ("none", "", AUSTRALIA),
    "2.1": ("row_leaf", NET_WORTH_QUINTILE, AUSTRALIA),
    "2.2": ("row_leaf", NET_WORTH_QUINTILE, AUSTRALIA),
    "2.3": ("row_top", "", AUSTRALIA),
    "2.4": ("none", "", AUSTRALIA),
    "3.1": (
        "columns",
        NET_WORTH_QUINTILE.replace("quintile", "decile"),
        AUSTRALIA,
    ),
    "3.2": ("row_top", "", AUSTRALIA),
    "3.3": ("row_top", "", AUSTRALIA),
    "3.4": ("row_top", "", AUSTRALIA),
    "3.5": ("row_top", "", AUSTRALIA),
    "3.6": ("columns", "low_economic_resource_group", AUSTRALIA),
    "3.7": ("columns", "low_economic_resource_group", AUSTRALIA),
    "4.1": ("columns", EDHI_QUINTILE, AUSTRALIA),
    "4.2": ("row_top", "", AUSTRALIA),
    "4.3": ("columns", NET_WORTH_QUINTILE, AUSTRALIA),
    "4.4": ("row_top", "", AUSTRALIA),
    "5.1": ("columns", EDHI_QUINTILE, AUSTRALIA),
    "5.2": ("columns", EDHI_QUINTILE, AUSTRALIA),
    "5.3": ("columns", EDHI_QUINTILE, AUSTRALIA),
    "5.4": ("columns", EDHI_QUINTILE, AUSTRALIA),
    "6.1": ("columns", GROSS_QUINTILE, AUSTRALIA),
    "6.2": ("columns", GROSS_QUINTILE, AUSTRALIA),
    "6.3": ("columns", GROSS_QUINTILE, AUSTRALIA),
    "6.4": ("columns", GROSS_QUINTILE, AUSTRALIA),
    "7.1": ("columns", NET_WORTH_QUINTILE, AUSTRALIA),
    "7.2": ("columns", NET_WORTH_QUINTILE, AUSTRALIA),
    "7.3": ("columns", NET_WORTH_QUINTILE, AUSTRALIA),
    "7.4": ("columns", NET_WORTH_QUINTILE, AUSTRALIA),
    "7.5": ("columns", NET_WORTH_QUINTILE, AUSTRALIA),
    "7.6": ("columns", NET_WORTH_QUINTILE, AUSTRALIA),
    "8.1": ("columns", "tenure_and_landlord_type", AUSTRALIA),
    "8.2": ("columns", "tenure_and_landlord_type", AUSTRALIA),
    "8.3": ("columns", "tenure_and_landlord_type", AUSTRALIA),
    "8.4": ("columns", "tenure_and_landlord_type", AUSTRALIA),
    "9.1": ("columns", "household_composition", AUSTRALIA),
    "9.2": ("columns", "household_composition", AUSTRALIA),
    "9.3": ("columns", "household_composition", AUSTRALIA),
    "9.4": ("columns", "household_composition", AUSTRALIA),
    "10.1": ("columns", "age_of_reference_person", AUSTRALIA),
    "10.2": ("columns", "age_of_reference_person", AUSTRALIA),
    "10.3": ("columns", "age_of_reference_person", AUSTRALIA),
    "10.4": ("columns", "age_of_reference_person", AUSTRALIA),
    "11.1": ("columns", "child_care_type", AUSTRALIA),
    "11.2": ("columns", "child_care_type", AUSTRALIA),
    "12.1": ("row_top", "", AUSTRALIA),
    "12.2": ("none", "", AUSTRALIA),
    "12.3": ("none", "", AUSTRALIA),
    "13.1": ("row_leaf", EDHI_QUINTILE, "columns"),
    "13.2": ("row_leaf", GROSS_QUINTILE, "columns"),
    "13.3": ("none", "", "columns"),
    "13.4": ("none", "", "columns"),
    "13.5": ("row_leaf", EDHI_QUINTILE, "columns"),
    "13.6": ("row_leaf", GROSS_QUINTILE, "columns"),
    "13.7": ("none", "", "columns"),
    "13.8": ("none", "", "columns"),
    "13.9": ("row_leaf", EDHI_QUINTILE, "columns"),
    "13.10": ("row_leaf", GROSS_QUINTILE, "columns"),
    "13.11": ("none", "", "columns"),
    "13.12": ("none", "", "columns"),
    "14.1": ("row_leaf", EDHI_QUINTILE, "New South Wales"),
    "14.2": ("row_leaf", EDHI_QUINTILE, "Victoria"),
    "14.3": ("row_leaf", EDHI_QUINTILE, "Queensland"),
    "14.4": ("row_leaf", EDHI_QUINTILE, "South Australia"),
    "14.5": ("row_leaf", EDHI_QUINTILE, "Western Australia"),
    "14.6": ("row_leaf", EDHI_QUINTILE, "Tasmania"),
    "14.7": ("row_leaf", EDHI_QUINTILE, "Northern Territory"),
    "14.8": ("row_leaf", EDHI_QUINTILE, "Australian Capital Territory"),
    "15.1": ("row_top", "", AUSTRALIA),
    "15.2": ("row_top", "", AUSTRALIA),
    "15.3": ("columns", "main_source_of_household_income", AUSTRALIA),
    "16.1": ("columns", EDHI_QUINTILE, AUSTRALIA),
    "16.2": ("columns", NET_WORTH_QUINTILE, AUSTRALIA),
    "16.3": ("none", "", "columns"),
    "16.4": ("columns", "number_of_financial_stress_indicators", AUSTRALIA),
}

# Column headers that restate the table's own classification rather than name
# a category within it. They are dropped from ``breakdown_value``; every other
# spanning header — "65 YEARS AND OVER", "RENTER", "FAMILY HOUSEHOLDS",
# "PRIVATE INCOME" — is a real group and is kept, because without it a leaf
# such as "Total" would not say what it totals.
TYPE_BANNERS = {
    "EQUIVALISED DISPOSABLE HOUSEHOLD INCOME QUINTILE",
    "EQUIVALISED DISPOSABLE HOUSEHOLD INCOME QUINTILES",
    "GROSS HOUSEHOLD INCOME QUINTILE",
    "GROSS HOUSEHOLD INCOME QUINTILES",
    "HOUSEHOLD INCOME QUINTILES",
    "HOUSEHOLD NET WORTH DECILES",
    "HOUSEHOLD NET WORTH QUINTILES",
    "NET WORTH QUINTILES",
    "NUMBER OF INDICATORS OF FINANCIAL STRESS EXPERIENCED",
    "TYPE OF CHILD CARE USED",
}

# Cube 13 abbreviates its column headers; cube 14 names a state in its title.
GEOGRAPHY_LABELS = {
    "Aust.": ("Australia", "national"),
    "NSW": ("New South Wales", "state or territory"),
    "Vic.": ("Victoria", "state or territory"),
    "Qld": ("Queensland", "state or territory"),
    "SA": ("South Australia", "state or territory"),
    "WA": ("Western Australia", "state or territory"),
    "Tas.": ("Tasmania", "state or territory"),
    "NT": ("Northern Territory", "state or territory"),
    "ACT": ("Australian Capital Territory", "state or territory"),
    "Greater Sydney": ("Greater Sydney", "greater capital city area"),
    "Greater Melbourne": ("Greater Melbourne", "greater capital city area"),
    "Greater Brisbane": ("Greater Brisbane", "greater capital city area"),
    "Greater Adelaide": ("Greater Adelaide", "greater capital city area"),
    "Greater Perth": ("Greater Perth", "greater capital city area"),
    "Greater Hobart": ("Greater Hobart", "greater capital city area"),
    "Greater Darwin": ("Greater Darwin", "greater capital city area"),
    "Rest of NSW": ("Rest of New South Wales", "rest of state"),
    "Rest of Vic.": ("Rest of Victoria", "rest of state"),
    "Rest of Qld": ("Rest of Queensland", "rest of state"),
    "Rest of SA": ("Rest of South Australia", "rest of state"),
    "Rest of WA": ("Rest of Western Australia", "rest of state"),
    "Rest of Tas.": ("Rest of Tasmania", "rest of state"),
    "Rest of NT": ("Rest of Northern Territory", "rest of state"),
}

STATE_LEVEL = {
    "Australia": "national",
    "New South Wales": "state or territory",
    "Victoria": "state or territory",
    "Queensland": "state or territory",
    "South Australia": "state or territory",
    "Western Australia": "state or territory",
    "Tasmania": "state or territory",
    "Northern Territory": "state or territory",
    "Australian Capital Territory": "state or territory",
}

# Labels for the coded columns, written to the dictionary table.
BREAKDOWN_TYPE_LABELS = {
    "total": "No breakdown; the estimate covers all households or persons",
    "age_of_oldest_person": "Age of the oldest person in the household",
    "age_of_person": "Age of the person",
    "area_of_usual_residence": (
        "Area of usual residence, splitting greater capital city areas from"
        " the rest of the state"
    ),
    "age_of_reference_person": "Age of the household reference person",
    "child_care_type": "Type of child care used",
    "contribution_of_government_pensions_to_gross_income": (
        "Contribution of government pensions and allowances to gross"
        " household income"
    ),
    "equivalised_disposable_household_income_quintile": (
        "Equivalised disposable household income quintile"
    ),
    "equivalised_disposable_household_income_range": (
        "Equivalised disposable household income per week"
    ),
    "gross_annual_income_decile": "Gross annual household income decile",
    "gross_household_income_quintile": "Gross household income quintile",
    "gross_household_income_range": "Gross household income per week",
    "household_composition": "Composition of the household",
    "household_type_and_age": (
        "Selected household types, with single-person households split by the"
        " age of the person"
    ),
    "labour_force_status": "Labour force status of household members",
    "low_economic_resource_group": (
        "Low economic resource group, contrasting the lowest income quintile,"
        " the lowest wealth quintile and low economic resource households"
    ),
    "main_source_of_household_income": "Main source of household income",
    "net_worth_decile": "Household net worth decile",
    "net_worth_quintile": "Household net worth quintile",
    "net_worth_range": "Household net worth range",
    "number_of_financial_stress_indicators": (
        "Number of indicators of financial stress experienced"
    ),
    "state_or_territory": "State or territory",
    "tenure_and_landlord_type": "Tenure type and, for renters, landlord type",
}

ESTIMATE_FLAG_LABELS = {
    "np": "Not published",
    "na": "Not available",
    "..": "Not applicable",
    "-": "Nil or rounded to zero",
    "*": "Estimate has a relative standard error of 25% to 50% and should be"
    " used with caution",
    "**": "Estimate has a relative standard error greater than 50% and is"
    " considered too unreliable for general use",
}
