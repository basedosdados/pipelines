{{
    config(
        alias="teacher_mathematics_grade_8",
        schema="world_iea_timss",
        materialized="table",
    )
}}
select
    safe_cast(year as string) year,
    safe_cast(country_m49 as string) country_m49,
    safe_cast(country_id as string) country_id,
    safe_cast(grade_id as string) grade_id,
    safe_cast(school_id as string) school_id,
    safe_cast(teach_id as int64) teach_id,
    safe_cast(link_id as int64) link_id,
    safe_cast(tealin_id as int64) tealin_id,
    safe_cast(course as int64) course,
    safe_cast(subject_id as string) subject_id,
    safe_cast(language_school_questionnaire as string) language_school_questionnaire,
    safe_cast(locale_school_questionnaire as string) locale_school_questionnaire,
    safe_cast(btbg01 as int64) btbg01,
    safe_cast(btbg02 as string) btbg02,
    safe_cast(btbg03 as string) btbg03,
    safe_cast(btbg04 as string) btbg04,
    safe_cast(btbg05a as string) btbg05a,
    safe_cast(btbg05b as string) btbg05b,
    safe_cast(btbg05c as string) btbg05c,
    safe_cast(btbg05d as string) btbg05d,
    safe_cast(btbg05e as string) btbg05e,
    safe_cast(btbg05f as string) btbg05f,
    safe_cast(btbg05g as string) btbg05g,
    safe_cast(btbg05h as string) btbg05h,
    safe_cast(btbg05i as string) btbg05i,
    safe_cast(btbg06a as string) btbg06a,
    safe_cast(btbg06b as string) btbg06b,
    safe_cast(btbg06c as string) btbg06c,
    safe_cast(btbg06d as string) btbg06d,
    safe_cast(btbg06e as string) btbg06e,
    safe_cast(btbg06f as string) btbg06f,
    safe_cast(btbg06g as string) btbg06g,
    safe_cast(btbg06h as string) btbg06h,
    safe_cast(btbg06i as string) btbg06i,
    safe_cast(btbg06j as string) btbg06j,
    safe_cast(btbg06k as string) btbg06k,
    safe_cast(btbg07a as string) btbg07a,
    safe_cast(btbg07b as string) btbg07b,
    safe_cast(btbg07c as string) btbg07c,
    safe_cast(btbg07d as string) btbg07d,
    safe_cast(btbg07e as string) btbg07e,
    safe_cast(btbg07f as string) btbg07f,
    safe_cast(btbg07g as string) btbg07g,
    safe_cast(btbg08a as string) btbg08a,
    safe_cast(btbg08b as string) btbg08b,
    safe_cast(btbg08c as string) btbg08c,
    safe_cast(btbg08d as string) btbg08d,
    safe_cast(btbg08e as string) btbg08e,
    safe_cast(btbg08f as string) btbg08f,
    safe_cast(btbg08g as string) btbg08g,
    safe_cast(btbg09a as string) btbg09a,
    safe_cast(btbg09b as string) btbg09b,
    safe_cast(btbg09c as string) btbg09c,
    safe_cast(btbg09d as string) btbg09d,
    safe_cast(btbg09e as string) btbg09e,
    safe_cast(btbg09f as string) btbg09f,
    safe_cast(btbg09g as string) btbg09g,
    safe_cast(btbg09h as string) btbg09h,
    safe_cast(btbg10 as int64) btbg10,
    safe_cast(btbg11 as int64) btbg11,
    safe_cast(btbg12a as string) btbg12a,
    safe_cast(btbg12b as string) btbg12b,
    safe_cast(btbg12c as string) btbg12c,
    safe_cast(btbg12d as string) btbg12d,
    safe_cast(btbg12e as string) btbg12e,
    safe_cast(btbg12f as string) btbg12f,
    safe_cast(btbg12g as string) btbg12g,
    safe_cast(btbg13a as string) btbg13a,
    safe_cast(btbg13b as string) btbg13b,
    safe_cast(btbg13c as string) btbg13c,
    safe_cast(btbg13d as string) btbg13d,
    safe_cast(btbg13e as string) btbg13e,
    safe_cast(btbg13f as string) btbg13f,
    safe_cast(btbg13g as string) btbg13g,
    safe_cast(btbg13h as string) btbg13h,
    safe_cast(btbg13i as string) btbg13i,
    safe_cast(btbm14 as int64) btbm14,
    safe_cast(btbm15a as string) btbm15a,
    safe_cast(btbm15b as string) btbm15b,
    safe_cast(btbm15c as string) btbm15c,
    safe_cast(btbm15d as string) btbm15d,
    safe_cast(btbm15e as string) btbm15e,
    safe_cast(btbm15f as string) btbm15f,
    safe_cast(btbm15g as string) btbm15g,
    safe_cast(btbm15h as string) btbm15h,
    safe_cast(btbm16 as string) btbm16,
    safe_cast(btbm17a as string) btbm17a,
    safe_cast(btbm17ba as string) btbm17ba,
    safe_cast(btbm17bb as string) btbm17bb,
    safe_cast(btbm17bc as string) btbm17bc,
    safe_cast(btbm17bd as string) btbm17bd,
    safe_cast(btbm17c as string) btbm17c,
    safe_cast(btbm17da as string) btbm17da,
    safe_cast(btbm17db as string) btbm17db,
    safe_cast(btbm17dc as string) btbm17dc,
    safe_cast(btbm17dd as string) btbm17dd,
    safe_cast(btbm17de as string) btbm17de,
    safe_cast(btbm17df as string) btbm17df,
    safe_cast(btbm18a as string) btbm18a,
    safe_cast(btbm18b as string) btbm18b,
    safe_cast(btbm18c as string) btbm18c,
    safe_cast(btbm18d as string) btbm18d,
    safe_cast(btbm19aa as string) btbm19aa,
    safe_cast(btbm19ab as string) btbm19ab,
    safe_cast(btbm19ac as string) btbm19ac,
    safe_cast(btbm19ad as string) btbm19ad,
    safe_cast(btbm19ae as string) btbm19ae,
    safe_cast(btbm19af as string) btbm19af,
    safe_cast(btbm19ag as string) btbm19ag,
    safe_cast(btbm19ba as string) btbm19ba,
    safe_cast(btbm19bb as string) btbm19bb,
    safe_cast(btbm19bc as string) btbm19bc,
    safe_cast(btbm19bd as string) btbm19bd,
    safe_cast(btbm19be as string) btbm19be,
    safe_cast(btbm19bf as string) btbm19bf,
    safe_cast(btbm19bg as string) btbm19bg,
    safe_cast(btbm19bh as string) btbm19bh,
    safe_cast(btbm19ca as string) btbm19ca,
    safe_cast(btbm19cb as string) btbm19cb,
    safe_cast(btbm19cc as string) btbm19cc,
    safe_cast(btbm19cd as string) btbm19cd,
    safe_cast(btbm19ce as string) btbm19ce,
    safe_cast(btbm19cf as string) btbm19cf,
    safe_cast(btbm19da as string) btbm19da,
    safe_cast(btbm19db as string) btbm19db,
    safe_cast(btbm19dc as string) btbm19dc,
    safe_cast(btbm19dd as string) btbm19dd,
    safe_cast(btbm20a as string) btbm20a,
    safe_cast(btbm20ba as string) btbm20ba,
    safe_cast(btbm20bb as string) btbm20bb,
    safe_cast(btbm20bc as string) btbm20bc,
    safe_cast(btbm20bd as string) btbm20bd,
    safe_cast(btbm20be as string) btbm20be,
    safe_cast(btbm21a as string) btbm21a,
    safe_cast(btbm21b as string) btbm21b,
    safe_cast(btbm21c as string) btbm21c,
    safe_cast(btbm21d as string) btbm21d,
    safe_cast(btbm21e as string) btbm21e,
    safe_cast(btbm22aa as string) btbm22aa,
    safe_cast(btbm22ba as string) btbm22ba,
    safe_cast(btbm22ab as string) btbm22ab,
    safe_cast(btbm22bb as string) btbm22bb,
    safe_cast(btbm22ac as string) btbm22ac,
    safe_cast(btbm22bc as string) btbm22bc,
    safe_cast(btbm22ad as string) btbm22ad,
    safe_cast(btbm22bd as string) btbm22bd,
    safe_cast(btbm22ae as string) btbm22ae,
    safe_cast(btbm22be as string) btbm22be,
    safe_cast(btbm22af as string) btbm22af,
    safe_cast(btbm22bf as string) btbm22bf,
    safe_cast(btbm22ag as string) btbm22ag,
    safe_cast(btbm22bg as string) btbm22bg,
    safe_cast(btbgeas as float64) btbgeas,
    safe_cast(btdgeas as string) btdgeas,
    safe_cast(btbgsos as float64) btbgsos,
    safe_cast(btdgsos as string) btdgsos,
    safe_cast(btbgtjs as float64) btbgtjs,
    safe_cast(btdgtjs as string) btdgtjs,
    safe_cast(btbglsn as float64) btbglsn,
    safe_cast(btdglsn as string) btdglsn,
    safe_cast(btdmmme as string) btdmmme,
    safe_cast(btdmnum as float64) btdmnum,
    safe_cast(btdmalg as float64) btdmalg,
    safe_cast(btdmgeo as float64) btdmgeo,
    safe_cast(btdmdat as float64) btdmdat,
    safe_cast(btdmhw as int64) btdmhw,
    safe_cast(version as string) version,
    safe_cast(scope as string) scope,
from
    {{ set_datalake_project("world_iea_timss_staging.teacher_mathematics_grade_8") }}
    as t
