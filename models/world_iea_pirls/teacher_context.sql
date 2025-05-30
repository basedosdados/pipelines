select
    safe_cast(country_iso3_code as string) country_iso3_code,
    safe_cast(country_id as string) country_id,
    safe_cast(population_id as string) population_id,
    safe_cast(standardized_grade_id as string) standardized_grade_id,
    safe_cast(grade_id as string) grade_id,
    safe_cast(school_id as string) school_id,
    safe_cast(teacher_id as string) teacher_id,
    safe_cast(teacher_link_number as string) teacher_link_number,
    safe_cast(teacher_link_id as string) teacher_link_id,
    safe_cast(language_teacher_questionnaire as string) language_teacher_questionnaire,
    safe_cast(
        locale_teacher_questionnaire_id as string
    ) locale_teacher_questionnaire_id,
    safe_cast(atbg01 as int64) atbg01,
    safe_cast(atbg02 as string) atbg02,
    safe_cast(atbg03 as string) atbg03,
    safe_cast(atbg04 as string) atbg04,
    safe_cast(atbg05aa as bool) atbg05aa,
    safe_cast(atbg05ab as bool) atbg05ab,
    safe_cast(atbg05ac as bool) atbg05ac,
    safe_cast(atbg05ad as bool) atbg05ad,
    safe_cast(atbg05ba as string) atbg05ba,
    safe_cast(atbg05bb as string) atbg05bb,
    safe_cast(atbg05bc as string) atbg05bc,
    safe_cast(atbg05bd as string) atbg05bd,
    safe_cast(atbg05be as string) atbg05be,
    safe_cast(atbg05bf as string) atbg05bf,
    safe_cast(atbg05bg as string) atbg05bg,
    safe_cast(atbg05bh as string) atbg05bh,
    safe_cast(atbg05bi as string) atbg05bi,
    safe_cast(atbg05bj as string) atbg05bj,
    safe_cast(atbg05bk as string) atbg05bk,
    safe_cast(atbg06 as string) atbg06,
    safe_cast(atbg07aa as bool) atbg07aa,
    safe_cast(atbg07ba as string) atbg07ba,
    safe_cast(atbg07ab as bool) atbg07ab,
    safe_cast(atbg07bb as string) atbg07bb,
    safe_cast(atbg07ac as bool) atbg07ac,
    safe_cast(atbg07bc as string) atbg07bc,
    safe_cast(atbg07ad as bool) atbg07ad,
    safe_cast(atbg07bd as string) atbg07bd,
    safe_cast(atbg07ae as bool) atbg07ae,
    safe_cast(atbg07be as string) atbg07be,
    safe_cast(atbg07af as bool) atbg07af,
    safe_cast(atbg07bf as string) atbg07bf,
    safe_cast(atbg07ag as bool) atbg07ag,
    safe_cast(atbg07bg as string) atbg07bg,
    safe_cast(atbg08a as string) atbg08a,
    safe_cast(atbg08b as string) atbg08b,
    safe_cast(atbg08c as string) atbg08c,
    safe_cast(atbg08d as string) atbg08d,
    safe_cast(atbg08e as string) atbg08e,
    safe_cast(atbg09a as string) atbg09a,
    safe_cast(atbg09b as string) atbg09b,
    safe_cast(atbg09c as string) atbg09c,
    safe_cast(atbg09d as string) atbg09d,
    safe_cast(atbg10a as string) atbg10a,
    safe_cast(atbg10b as string) atbg10b,
    safe_cast(atbg10c as string) atbg10c,
    safe_cast(atbg10d as string) atbg10d,
    safe_cast(atbg10e as string) atbg10e,
    safe_cast(atbg10f as string) atbg10f,
    safe_cast(atbg10g as string) atbg10g,
    safe_cast(atbg10h as string) atbg10h,
    safe_cast(atbg10i as string) atbg10i,
    safe_cast(atbg10j as string) atbg10j,
    safe_cast(atbg10k as string) atbg10k,
    safe_cast(atbg10l as string) atbg10l,
    safe_cast(atbg11a as string) atbg11a,
    safe_cast(atbg11b as string) atbg11b,
    safe_cast(atbg11c as string) atbg11c,
    safe_cast(atbg11d as string) atbg11d,
    safe_cast(atbg11e as string) atbg11e,
    safe_cast(atbg11f as string) atbg11f,
    safe_cast(atbg11g as string) atbg11g,
    safe_cast(atbg11h as string) atbg11h,
    safe_cast(atbg11i as string) atbg11i,
    safe_cast(atbg12a as string) atbg12a,
    safe_cast(atbg12b as string) atbg12b,
    safe_cast(atbg12c as string) atbg12c,
    safe_cast(atbg12d as string) atbg12d,
    safe_cast(atbg12e as string) atbg12e,
    safe_cast(atbg12f as string) atbg12f,
    safe_cast(atbr01a as int64) atbr01a,
    safe_cast(atbr01b as int64) atbr01b,
    safe_cast(atbr02a as int64) atbr02a,
    safe_cast(atbr02b as int64) atbr02b,
    safe_cast(atbr03a as string) atbr03a,
    safe_cast(atbr03b as string) atbr03b,
    safe_cast(atbr03c as string) atbr03c,
    safe_cast(atbr03d as string) atbr03d,
    safe_cast(atbr03e as string) atbr03e,
    safe_cast(atbr03f as string) atbr03f,
    safe_cast(atbr03g as string) atbr03g,
    safe_cast(atbr03h as string) atbr03h,
    safe_cast(atbr04 as int64) atbr04,
    safe_cast(atbr05 as int64) atbr05,
    safe_cast(atbr06a as string) atbr06a,
    safe_cast(atbr06b as string) atbr06b,
    safe_cast(atbr06c as string) atbr06c,
    safe_cast(atbr06d as string) atbr06d,
    safe_cast(atbr06e as string) atbr06e,
    safe_cast(atbr07aa as string) atbr07aa,
    safe_cast(atbr07ab as string) atbr07ab,
    safe_cast(atbr07ac as string) atbr07ac,
    safe_cast(atbr07ad as string) atbr07ad,
    safe_cast(atbr07ba as string) atbr07ba,
    safe_cast(atbr07bb as string) atbr07bb,
    safe_cast(atbr07bc as string) atbr07bc,
    safe_cast(atbr07bd as string) atbr07bd,
    safe_cast(atbr08a as string) atbr08a,
    safe_cast(atbr08b as string) atbr08b,
    safe_cast(atbr08c as string) atbr08c,
    safe_cast(atbr08d as string) atbr08d,
    safe_cast(atbr08e as string) atbr08e,
    safe_cast(atbr08f as string) atbr08f,
    safe_cast(atbr08g as string) atbr08g,
    safe_cast(atbr08h as string) atbr08h,
    safe_cast(atbr09a as string) atbr09a,
    safe_cast(atbr09b as string) atbr09b,
    safe_cast(atbr09c as string) atbr09c,
    safe_cast(atbr09d as string) atbr09d,
    safe_cast(atbr09e as string) atbr09e,
    safe_cast(atbr09f as string) atbr09f,
    safe_cast(atbr09g as string) atbr09g,
    safe_cast(atbr09h as string) atbr09h,
    safe_cast(atbr09i as string) atbr09i,
    safe_cast(atbr10a as string) atbr10a,
    safe_cast(atbr10b as string) atbr10b,
    safe_cast(atbr10c as string) atbr10c,
    safe_cast(atbr10d as string) atbr10d,
    safe_cast(atbr10e as string) atbr10e,
    safe_cast(atbr10f as string) atbr10f,
    safe_cast(atbr10g as string) atbr10g,
    safe_cast(atbr10h as string) atbr10h,
    safe_cast(atbr10i as string) atbr10i,
    safe_cast(atbr10j as string) atbr10j,
    safe_cast(atbr10k as string) atbr10k,
    safe_cast(atbr10l as string) atbr10l,
    safe_cast(atbr11a as string) atbr11a,
    safe_cast(atbr11b as string) atbr11b,
    safe_cast(atbr11c as string) atbr11c,
    safe_cast(atbr11d as string) atbr11d,
    safe_cast(atbr11e as string) atbr11e,
    safe_cast(atbr12a as bool) atbr12a,
    safe_cast(atbr12ba as bool) atbr12ba,
    safe_cast(atbr12bb as bool) atbr12bb,
    safe_cast(atbr12bc as bool) atbr12bc,
    safe_cast(atbr12bd as bool) atbr12bd,
    safe_cast(atbr12c as string) atbr12c,
    safe_cast(atbr12da as string) atbr12da,
    safe_cast(atbr12db as string) atbr12db,
    safe_cast(atbr12dc as string) atbr12dc,
    safe_cast(atbr12ea as string) atbr12ea,
    safe_cast(atbr12eb as string) atbr12eb,
    safe_cast(atbr12ec as string) atbr12ec,
    safe_cast(atbr12ed as string) atbr12ed,
    safe_cast(atbr12ee as string) atbr12ee,
    safe_cast(atbr13a as bool) atbr13a,
    safe_cast(atbr13b as string) atbr13b,
    safe_cast(atbr13c as string) atbr13c,
    safe_cast(atbr13d as string) atbr13d,
    safe_cast(atbr13e as bool) atbr13e,
    safe_cast(atbr14 as string) atbr14,
    safe_cast(atbr15 as string) atbr15,
    safe_cast(atbr16 as string) atbr16,
    safe_cast(atbr17a as string) atbr17a,
    safe_cast(atbr17b as string) atbr17b,
    safe_cast(atbr17c as string) atbr17c,
    safe_cast(atbr18a as string) atbr18a,
    safe_cast(atbr18b as string) atbr18b,
    safe_cast(atbr18c as string) atbr18c,
    safe_cast(atbr18d as string) atbr18d,
    safe_cast(atbr18e as string) atbr18e,
    safe_cast(atbr19 as string) atbr19,
    safe_cast(atbgeas as float64) atbgeas,
    safe_cast(atdgeas as string) atdgeas,
    safe_cast(atbgsos as float64) atbgsos,
    safe_cast(atdgsos as string) atdgsos,
    safe_cast(atbgtjs as float64) atbgtjs,
    safe_cast(atdgtjs as string) atdgtjs,
    safe_cast(atbgsli as float64) atbgsli,
    safe_cast(atdgsli as string) atdgsli,
    safe_cast(atdglihy as float64) atdglihy,
    safe_cast(atdgrihy as float64) atdgrihy,
    safe_cast(version as string) version,
    safe_cast(scope as string) scope,
    safe_cast(pirls_type as string) pirls_type,
from {{ set_datalake_project("world_iea_pirls_staging.teacher_context") }} as t
