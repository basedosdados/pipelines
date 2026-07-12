#!/usr/bin/env python3
"""Deterministic ESS mnemonic -> descriptive English snake_case name generator.
Reproducible so the SAME mnemonic yields the SAME name in every round.
Strategy: curated OVERRIDE for core/important vars; label-driven normalizer for
the long tail; country-suffixed labels ("<concept>, <Country>") handled uniformly."""

import collections
import json
import re

COUNTRY = {  # snake_case country names as they appear at end of ESS labels
    "albania",
    "austria",
    "belgium",
    "bulgaria",
    "switzerland",
    "cyprus",
    "czechia",
    "germany",
    "denmark",
    "estonia",
    "spain",
    "finland",
    "france",
    "united kingdom",
    "georgia",
    "greece",
    "croatia",
    "hungary",
    "ireland",
    "iceland",
    "israel",
    "italy",
    "lithuania",
    "luxembourg",
    "latvia",
    "montenegro",
    "north macedonia",
    "netherlands",
    "norway",
    "poland",
    "portugal",
    "romania",
    "serbia",
    "russian federation",
    "sweden",
    "slovenia",
    "slovakia",
    "ukraine",
    "turkey",
    "kosovo",
}

# hand-picked names for the most-used / awkward-to-derive variables
OVERRIDE = {
    # media & social trust
    "nwspol": "minutes_news_politics",
    "netusoft": "internet_use_frequency",
    "netustm": "minutes_internet_use",
    "ppltrst": "most_people_can_be_trusted",
    "pplfair": "most_people_try_to_be_fair",
    "pplhlp": "most_people_helpful",
    # politics (core)
    "polintr": "interest_in_politics",
    "psppsgva": "political_system_allows_say",
    "actrolga": "able_to_take_active_role",
    "psppipla": "political_system_allows_influence",
    "cptppola": "confident_participate_politics",
    "trstprl": "trust_parliament",
    "trstlgl": "trust_legal_system",
    "trstplc": "trust_police",
    "trstplt": "trust_politicians",
    "trstprt": "trust_political_parties",
    "trstep": "trust_european_parliament",
    "trstun": "trust_united_nations",
    "vote": "voted_last_national_election",
    "contplt": "contacted_politician",
    "donprty": "donated_to_party",
    "badge": "worn_campaign_badge",
    "sgnptit": "signed_petition",
    "pbldmna": "participated_lawful_demonstration",
    "bctprd": "boycotted_products",
    "pstplonl": "posted_shared_politics_online",
    "clsprty": "feel_close_to_party",
    "prtdgcl": "how_close_to_party",
    "lrscale": "left_right_scale",
    "stflife": "life_satisfaction",
    "stfeco": "satisfaction_economy",
    "stfgov": "satisfaction_government",
    "stfdem": "satisfaction_democracy",
    "stfedu": "satisfaction_education",
    "stfhlth": "satisfaction_health_services",
    "gincdif": "government_reduce_income_differences",
    "freehms": "gays_free_to_live_as_wish",
    "hmsfmlsh": "ashamed_gay_family_member",
    "hmsacld": "gay_couples_adopt_rights",
    "euftf": "european_unification_go_further",
    "imsmetn": "allow_immigrants_same_ethnicity",
    "imdfetn": "allow_immigrants_different_ethnicity",
    "impcntr": "allow_immigrants_poorer_countries",
    "imbgeco": "immigration_good_for_economy",
    "imueclt": "immigration_undermines_culture",
    "imwbcnt": "immigrants_make_country_better",
    # wellbeing / exclusion / religion / identity (core)
    "happy": "happiness",
    "sclmeet": "frequency_social_meetings",
    "inprdsc": "number_people_discuss_personal",
    "sclact": "social_activities_vs_peers",
    "crmvct": "victim_of_crime",
    "aesfdrk": "feeling_safe_walking_dark",
    "health": "subjective_general_health",
    "hlthhmp": "hampered_by_illness",
    "rlgblg": "belongs_to_religion",
    "rlgdgr": "how_religious",
    "rlgatnd": "religious_attendance_frequency",
    "pray": "frequency_of_praying",
    "happy_dummy": "happiness",
    "dscrgrp": "member_discriminated_group",
    "ctzcntr": "citizen_of_country",
    "brncntr": "born_in_country",
    "livecnta": "year_arrived_country",
    "lnghom1": "language_home_first",
    "lnghom2": "language_home_second",
    "blgetmg": "belong_ethnic_minority",
    "facntr": "father_born_in_country",
    "mocntr": "mother_born_in_country",
    "atchctr": "emotional_attachment_country",
    "atcherp": "emotional_attachment_europe",
    # sociodemographics / household grid (core)
    "gndr": "gender",
    "yrbrn": "year_of_birth",
    "agea": "age",
    "maritalb": "legal_marital_status",
    "chldhhe": "children_living_at_home",
    "domicil": "domicile_type",
    "eduyrs": "years_of_education",
    "pdwrk": "paid_work_last_week",
    "edctn": "in_education_last_week",
    "uempla": "unemployed_active_last_week",
    "uempli": "unemployed_inactive_last_week",
    "dsbld": "permanently_disabled_last_week",
    "rtrd": "retired_last_week",
    "hswrk": "housework_last_week",
    "mainact": "main_activity_last_7_days",
    "wrkctra": "employment_contract_type",
    "estsz": "establishment_size",
    "emplrel": "employment_relation",
    "emplno": "number_of_employees",
    "wkhct": "contracted_weekly_hours",
    "wkhtot": "total_weekly_hours",
    "hincsrca": "main_source_household_income",
    "hinctnta": "household_income_decile",
    "hincfel": "feeling_about_household_income",
    # human values (Schwartz portrait items) -- R11 mnemonics end in 'a'
    "ipcrtiva": "value_creativity",
    "impricha": "value_wealth",
    "ipeqopta": "value_equality",
    "ipshabta": "value_show_abilities",
    "impsafea": "value_safety",
    "impdiffa": "value_variety",
    "ipadvnta": "value_adventure",
    "ipbhprpa": "value_behaving_properly",
    "iprspota": "value_respect_from_others",
    "iplylfra": "value_loyalty_to_friends",
    "impenva": "value_environment",
    "imptrada": "value_tradition",
    "ipfrulea": "value_follow_rules",
    "ipudrsta": "value_understanding_others",
    "ipmodsta": "value_modesty",
    "ipgdtima": "value_good_time",
    "impfreea": "value_freedom",
    "iphlppla": "value_helping_others",
    "ipsucesa": "value_success",
    "ipstrgva": "value_strong_government",
    "impfuna": "value_fun",
    # administrative / weights
    "name": "dataset_title",
    "essround": "ess_round",
    "edition": "edition",
    "proddate": "production_date",
    "idno": "respondent_id",
    "cntry": "country_code_iso2",
    "dweight": "design_weight",
    "pspwght": "poststratification_weight",
    "pweight": "population_size_weight",
    "anweight": "analysis_weight",
    "region": "region_code",
    "prob": "selection_probability",
    "stratum": "sampling_stratum",
    "psu": "primary_sampling_unit",
}

ABBREV = [  # applied in order, whole-word-ish, on the lowercased label
    (r"don'?t", "dont"),
    (r"can'?t", "cant"),
    (r"won'?t", "wont"),
    (r"party voted for in last national election", "party voted"),
    (r"party voted for in last election", "party voted"),
    (r"\bwhich party.*close.*", "party feel close to"),
    (r"\bhighest level of education\b", "highest education"),
    (r"\blast national election\b", "last election"),
    (r"european standard classification of occupations.*", "isco"),
    (r"\bhow likely\b", "likelihood"),
    (r"\bhow often\b", "frequency"),
    (r"\bhow many\b", "number"),
    (r"\bnumber of\b", "number"),
    (r"\brespondent'?s?\b", "respondent"),
    (r"\bpartner'?s\b", "partner"),
    (r"\bfather'?s\b", "father"),
    (r"\bmother'?s\b", "mother"),
    (r"\bcountry'?s\b", "country"),
    (r"\bpeople'?s\b", "people"),
]

# ordinal words -> number, for household-grid labels
ORD = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
    "fifth": 5,
    "sixth": 6,
    "seventh": 7,
    "eighth": 8,
    "ninth": 9,
    "tenth": 10,
    "eleventh": 11,
    "twelfth": 12,
    "thirteenth": 13,
    "fourteenth": 14,
    "fifteenth": 15,
    "sixteenth": 16,
}

# household-grid: mnemonic = stem + trailing digit -> household_member_<N>_<suffix>
GRID = {
    "rshipa": "relationship",
    "gndr": "gender",
    "yrbrn": "year_of_birth",
    "agea": "age",
    "rshpsb": "relationship",
}

# colon-battery: "<prefix>: <item>" -> "<short>_<item>" (item preserved as distinguisher)
COLON = [
    (r"^treatments used for own health.*", "treatment"),
    (r"^no medical consultation or treatment, reason", "no_medical_reason"),
    (r"^problems? with accomm?odation", "accommodation_problem"),
    (r"^improve knowledge/skills", "improve_skills"),
    (r"^in any job, ever exposed to", "job_exposed_to"),
    (r"^health problems, hampered", "health_problem_hampered"),
    (r"^health problems, last 12", "health_problem"),
    (r"^discrimination of respondent", "discrimination_group"),
    (r"^partner doing last 7 days", "partner_activity"),
    (r"^doing last 7 days", "activity"),
    (r"^discussed health, last 12", "discussed_health_with"),
    (r"^language most often spoken at home", "language_home"),
]
STOP = {
    "the",
    "a",
    "an",
    "of",
    "in",
    "on",
    "to",
    "for",
    "or",
    "and",
    "as",
    "at",
    "by",
    "with",
    "you",
    "your",
    "how",
    "are",
    "is",
    "be",
    "been",
    "being",
    "that",
    "this",
    "which",
    "whole",
    "about",
    "'s",
    "s",
    "last",
    "was",
}


def norm(text):
    t = text.lower().strip()
    for pat, rep in ABBREV:
        t = re.sub(pat, rep, t)
    t = re.sub(r"[^a-z0-9]+", " ", t)  # non-alnum -> space
    toks = [w for w in t.split() if w and w not in STOP]
    name = "_".join(toks)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def base_name(mn, label):
    if mn in OVERRIDE:
        return OVERRIDE[mn]
    lab = label.strip()
    lab = lab.split("[")[0].strip()  # drop "[country]" placeholders

    # 1) household grid: mnemonic stem + trailing number
    gm = re.match(r"^([a-z]+?)(\d{1,2})$", mn)
    if gm and gm.group(1) in GRID:
        return f"household_member_{int(gm.group(2))}_{GRID[gm.group(1)]}"

    # 2) colon-battery: "<prefix>: <item>"  -> "<short>_<item>"
    if ":" in lab:
        pre, item = lab.split(":", 1)
        pre_s, item_s = pre.strip(), item.strip()
        # 2a) country-suffixed concept before the colon (e.g. German/UK education) ->
        #     treat as country var, drop the country-language tail after the colon
        cm = re.search(r"^(.*),\s*([A-Za-z .]+)$", pre_s)
        if cm and cm.group(2).strip().lower() in COUNTRY:
            concept = norm(cm.group(1))
            country = re.sub(
                r"[^a-z0-9]+", "_", cm.group(2).strip().lower()
            ).strip("_")
            return f"{concept}_{country}"
        # 2b) known battery -> short prefix + item
        for pat, short in COLON:
            if re.search(pat, pre_s.lower()):
                return f"{short}_{norm(item_s)}".strip("_")
        # 2c) default: keep short concept + distinguishing item
        concept = "_".join(norm(pre_s).split("_")[:3])
        itemn = "_".join(norm(item_s).split("_")[:4])
        return f"{concept}_{itemn}".strip("_") or norm(pre_s)

    # 3) country-suffixed label: "<concept>, <Country>"
    m = re.search(r"^(.*),\s*([A-Za-z .]+)$", lab)
    if m and m.group(2).strip().lower() in COUNTRY:
        concept = norm(m.group(1))
        country = re.sub(r"[^a-z0-9]+", "_", m.group(2).strip().lower()).strip(
            "_"
        )
        return f"{concept}_{country}"

    n = norm(lab)
    return n or mn


def main():
    with open("variables.json") as fh:
        rows = json.load(fh)
    names = {}
    used = collections.Counter()
    out = []
    for r in rows:
        mn = r["mnemonic"]
        nm = base_name(mn, r["label"])
        if len(nm) > 48:  # cap very long derived names
            nm = "_".join(nm.split("_")[:6])
        # enforce uniqueness
        if nm in used:
            used[nm] += 1
            nm2 = f"{nm}_{used[nm]}"
        else:
            used[nm] = 1
            nm2 = nm
        names[mn] = nm2
        out.append(
            (
                mn,
                nm2,
                r["bq_type"],
                "yes" if r["n_cats"] > 0 else "no",
                r["module"],
                r["label"],
            )
        )

    with open("names.tsv", "w", encoding="utf-8") as f:
        f.write(
            "mnemonic\tname\tbq_type\tcovered_by_dictionary\tmodule\tlabel\n"
        )
        for mn, nm, bt, cbd, mod, lab in out:
            f.write(f"{mn}\t{nm}\t{bt}\t{cbd}\t{mod}\t{lab}\n")

    # QC report
    dups = [n for n, c in collections.Counter(names.values()).items() if c > 1]
    long = [(m, n) for m, n in names.items() if len(n) > 40]
    overr = sum(1 for m in names if m in OVERRIDE)
    print(
        f"generated {len(names)} names | overrides used {overr} | dup-final {len(dups)}"
    )
    print(f"names >40 chars: {len(long)}")
    print("\n25 sample derived (non-override) names:")
    shown = 0
    for mn, nm, _bt, _cbd, _mod, lab in out:
        if mn not in OVERRIDE and shown < 25:
            print(f"  {mn:10s} {nm:42s} | {lab[:46]}")
            shown += 1


if __name__ == "__main__":
    main()
