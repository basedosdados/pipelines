# Gate B — phase-1 rebuild log (RAM-safe, 16 GB host)

| table-year | result | note |
|---|---|---|
| — candidatos — | | |
| candidatos_1994 | MATCH | |
| candidatos_1996 | MATCH | |
| candidatos_1998 | MATCH | |
| candidatos_2000 | MATCH | |
| candidatos_2002 | MATCH | |
| candidatos_2004 | MATCH | |
| candidatos_2006 | MATCH | |
| candidatos_2008 | MATCH | |
| candidatos_2010 | MATCH | |
| candidatos_2012 | MATCH | |
| candidatos_2014 | MATCH | |
| candidatos_2016 | MATCH | |
| candidatos_2018 | MATCH | |
| candidatos_2020 | MATCH | |
| candidatos_2022 | MATCH | |
| candidatos_2024 | MATCH | |
| — partidos — | | |
| partidos_1990 | ERROR | FileNotFoundError: No file found for partidos 1990 AC |
| partidos_1994 | MATCH | |
| partidos_1996 | MATCH | |
| partidos_1998 | MATCH | |
| partidos_2000 | MATCH | |
| partidos_2002 | MATCH | |
| partidos_2004 | MATCH | |
| partidos_2006 | MATCH | |
| partidos_2008 | MATCH | |
| partidos_2010 | MATCH | |
| partidos_2012 | MATCH | |
| partidos_2014 | MATCH | |
| partidos_2016 | MATCH | |
| partidos_2018 | MATCH | |
| partidos_2020 | DIFF-CELLS rows 139614 (schema+count match) | |
| partidos_2022 | MATCH | |
| partidos_2024 | MATCH | |
| — vagas — | | |
| vagas_1994 | MATCH | |
| vagas_1996 | MATCH | |
| vagas_1998 | MATCH | |
| vagas_2000 | MATCH | |
| vagas_2002 | MATCH | |
| vagas_2004 | MATCH | |
| vagas_2006 | MATCH | |
| vagas_2008 | MATCH | |
| vagas_2010 | MATCH | |
| vagas_2012 | MATCH | |
| vagas_2014 | MATCH | |
| vagas_2016 | MATCH | |
| vagas_2018 | MATCH | |
| vagas_2020 | MATCH | |
| vagas_2022 | MATCH | |
| vagas_2024 | MATCH | |
| — bens — | | |
| bens_candidato_2006 | DIFF rows 86946 vs 86725 | |
| bens_candidato_2008 | MATCH | |
| bens_candidato_2010 | DIFF rows 81226 vs 81050 | |
| bens_candidato_2012 | MATCH | |
| bens_candidato_2014 | DIFF rows 83055 vs 82837 | |
| bens_candidato_2016 | MATCH | |
| bens_candidato_2018 | DIFF rows 93527 vs 93212 | |
| bens_candidato_2020 | MATCH | |
| bens_candidato_2022 | DIFF rows 92561 vs 92321 | |
| bens_candidato_2024 | MATCH | |
| — detalhes_mun_zona — | | |
| detalhes_votacao_municipio_zona_1994 | MATCH | |
| detalhes_votacao_municipio_zona_1996 | MATCH | |
| detalhes_votacao_municipio_zona_1998 | MATCH | |
| detalhes_votacao_municipio_zona_2000 | MATCH | |
| detalhes_votacao_municipio_zona_2002 | MATCH | |
| detalhes_votacao_municipio_zona_2004 | MATCH | |
| detalhes_votacao_municipio_zona_2006 | MATCH | |
| detalhes_votacao_municipio_zona_2008 | MATCH | |
| detalhes_votacao_municipio_zona_2010 | MATCH | |
| detalhes_votacao_municipio_zona_2012 | MATCH | |
| detalhes_votacao_municipio_zona_2014 | MATCH | |
| detalhes_votacao_municipio_zona_2016 | MATCH | |
| detalhes_votacao_municipio_zona_2018 | MATCH | |
| detalhes_votacao_municipio_zona_2020 | MATCH | |
| detalhes_votacao_municipio_zona_2022 | MATCH | |
| detalhes_votacao_municipio_zona_2024 | MATCH | |
| — detalhes_secao — | | |
| detalhes_votacao_secao_1998 | MATCH | |
| detalhes_votacao_secao_2000 | MATCH | |
| detalhes_votacao_secao_2002 | MATCH | |
| detalhes_votacao_secao_2004 | MATCH | |
| detalhes_votacao_secao_2006 | MATCH | |
| detalhes_votacao_secao_2008 | MATCH | |
| detalhes_votacao_secao_2010 | MATCH | |
| detalhes_votacao_secao_2012 | MATCH | |
| detalhes_votacao_secao_2014 | MATCH | |
| detalhes_votacao_secao_2016 | MATCH | |
| detalhes_votacao_secao_2018 | MATCH | |
| detalhes_votacao_secao_2020 | MATCH | |
| detalhes_votacao_secao_2022 | MATCH | |
| detalhes_votacao_secao_2024 | MATCH | |
| — perfil_mun_zona — | | |
| perfil_eleitorado_municipio_zona_1994 | MATCH | |
| perfil_eleitorado_municipio_zona_1996 | MATCH | |
| perfil_eleitorado_municipio_zona_1998 | MATCH | |
| perfil_eleitorado_municipio_zona_2000 | MATCH | |
| perfil_eleitorado_municipio_zona_2002 | MATCH | |
| perfil_eleitorado_municipio_zona_2004 | MATCH | |
| perfil_eleitorado_municipio_zona_2006 | MATCH | |
| perfil_eleitorado_municipio_zona_2008 | MATCH | |
| perfil_eleitorado_municipio_zona_2010 | MATCH | |
| perfil_eleitorado_municipio_zona_2012 | MATCH | |
| perfil_eleitorado_municipio_zona_2014 | MATCH | |
| perfil_eleitorado_municipio_zona_2016 | MATCH | |
| perfil_eleitorado_municipio_zona_2018 | MATCH | |
| perfil_eleitorado_municipio_zona_2020 | MATCH | |
| perfil_eleitorado_municipio_zona_2022 | MATCH | |
| perfil_eleitorado_municipio_zona_2024 | MATCH | |
| — perfil_secao — | SKIP-BIG | 60M-row builds exceed 16 GB; aa_eleicao key inert on ANO_ELEICAO local vintage → byte-identical to March parquets (Gate A cell-verified). Production build in work order 05 needs streaming/bigger host |
| — local_votacao — | | |
| perfil_eleitorado_local_votacao_2010 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2012 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2014 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2016 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2018 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2020 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2022 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2024 | NO_MARCH_REF | |
| — rcmz — | | |
| resultados_candidato_municipio_zona_1994 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_1996 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_1998 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_2000 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_2002 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_2004 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_2006 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_2008 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_2010 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_2012 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_2014 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_2016 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_2018 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_2020 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_2022 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| resultados_candidato_municipio_zona_2024 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_candidato' |
| — rpmz — | | |
| resultados_partido_municipio_zona_1994 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_1996 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_1998 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_2000 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_2002 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_2004 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_2006 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_2008 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_2010 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_2012 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_2014 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_2016 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_2018 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_2020 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_2022 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| resultados_partido_municipio_zona_2024 | ERROR | AttributeError: module 'sub.results_mun_zone' has no attribute 'build_partido' |
| — receitas — | | |
| receitas_candidato_2002 | MATCH | |
| receitas_candidato_2004 | MATCH | |
| receitas_candidato_2006 | MATCH | |
| receitas_candidato_2008 | MATCH | |
| receitas_candidato_2010 | MATCH | |
| receitas_candidato_2012 | MATCH | |
| receitas_candidato_2014 | MATCH | |
| receitas_candidato_2016 | MATCH | |
| receitas_candidato_2018 | MATCH | |
| receitas_candidato_2020 | MATCH | |
| receitas_candidato_2022 | MATCH | |
| receitas_candidato_2024 | MATCH | |
| — despesas — | | |
| despesas_candidato_2002 | MATCH | |
| despesas_candidato_2004 | MATCH | |
| despesas_candidato_2006 | MATCH | |
| despesas_candidato_2008 | MATCH | |
| despesas_candidato_2010 | MATCH | |
| despesas_candidato_2012 | MATCH | |
| despesas_candidato_2014 | MATCH | |
| despesas_candidato_2016 | MATCH | |
| despesas_candidato_2018 | MATCH | |
| despesas_candidato_2020 | MATCH | |
| despesas_candidato_2022 | MATCH | |
| despesas_candidato_2024 | MATCH | |
| — resultados_secao — | SKIP-BIG | 60M-row builds exceed 16 GB; aa_eleicao key inert on ANO_ELEICAO local vintage → byte-identical to March parquets (Gate A cell-verified). Production build in work order 05 needs streaming/bigger host |
| — candidatos — | | |
| — partidos — | | |
| partidos_1990 | MATCH | |
| — vagas — | | |
| — bens — | | |
| — detalhes_mun_zona — | | |
| — detalhes_secao — | | |
| — perfil_mun_zona — | | |
| — perfil_secao — | SKIP-BIG | 60M-row builds exceed 16 GB; aa_eleicao key inert on ANO_ELEICAO local vintage → byte-identical to March parquets (Gate A cell-verified). Production build in work order 05 needs streaming/bigger host |
| — local_votacao — | | |
| — rcmz — | | |
| resultados_candidato_municipio_zona_1994 | MATCH | |
| resultados_candidato_municipio_zona_1996 | MATCH | |
| resultados_candidato_municipio_zona_1998 | MATCH | |
| resultados_candidato_municipio_zona_2000 | MATCH | |
| resultados_candidato_municipio_zona_2002 | MATCH | |
| resultados_candidato_municipio_zona_2004 | MATCH | |
| resultados_candidato_municipio_zona_2006 | MATCH | |
| resultados_candidato_municipio_zona_2008 | MATCH | |
| resultados_candidato_municipio_zona_2010 | MATCH | |
| resultados_candidato_municipio_zona_2012 | MATCH | |
| resultados_candidato_municipio_zona_2014 | MATCH | |
| resultados_candidato_municipio_zona_2016 | MATCH | |
| resultados_candidato_municipio_zona_2018 | MATCH | |
| resultados_candidato_municipio_zona_2020 | MATCH | |
| resultados_candidato_municipio_zona_2022 | MATCH | |
| resultados_candidato_municipio_zona_2024 | MATCH | |
| — rpmz — | | |
| resultados_partido_municipio_zona_1994 | MATCH | |
| resultados_partido_municipio_zona_1996 | MATCH | |
| resultados_partido_municipio_zona_1998 | MATCH | |
| resultados_partido_municipio_zona_2000 | MATCH | |
| resultados_partido_municipio_zona_2002 | MATCH | |
| resultados_partido_municipio_zona_2004 | MATCH | |
| resultados_partido_municipio_zona_2006 | MATCH | |
| resultados_partido_municipio_zona_2008 | MATCH | |
| resultados_partido_municipio_zona_2010 | MATCH | |
| resultados_partido_municipio_zona_2012 | MATCH | |
| resultados_partido_municipio_zona_2014 | MATCH | |
| resultados_partido_municipio_zona_2016 | MATCH | |
| resultados_partido_municipio_zona_2018 | MATCH | |
| resultados_partido_municipio_zona_2020 | MATCH | |
| resultados_partido_municipio_zona_2022 | MATCH | |
| resultados_partido_municipio_zona_2024 | MATCH | |
| — receitas — | | |
| — despesas — | | |
| — resultados_secao — | SKIP-BIG | 60M-row builds exceed 16 GB; aa_eleicao key inert on ANO_ELEICAO local vintage → byte-identical to March parquets (Gate A cell-verified). Production build in work order 05 needs streaming/bigger host |
| — candidatos — | | |
| candidatos_1994 | MATCH | |
| candidatos_1996 | MATCH | |
| candidatos_1998 | MATCH | |
| candidatos_2000 | MATCH | |
| candidatos_2002 | MATCH | |
| candidatos_2004 | MATCH | |
| candidatos_2006 | MATCH | |
| candidatos_2008 | MATCH | |
| candidatos_2010 | MATCH | |
| candidatos_2012 | MATCH | |
| candidatos_2014 | MATCH | |
| candidatos_2016 | MATCH | |
| candidatos_2018 | MATCH | |
| candidatos_2020 | MATCH | |
| candidatos_2022 | MATCH | |
| candidatos_2024 | MATCH | |
| — partidos — | | |
| partidos_1990 | MATCH | |
| partidos_1994 | MATCH | |
| partidos_1996 | MATCH | |
| partidos_1998 | MATCH | |
| partidos_2000 | MATCH | |
| partidos_2002 | MATCH | |
| partidos_2004 | MATCH | |
| partidos_2006 | MATCH | |
| partidos_2008 | MATCH | |
| partidos_2010 | MATCH | |
| partidos_2012 | MATCH | |
| partidos_2014 | MATCH | |
| partidos_2016 | MATCH | |
| partidos_2018 | MATCH | |
| partidos_2020 | DIFF-CELLS rows 139614 (schema+count match) | |
| partidos_2022 | MATCH | |
| partidos_2024 | MATCH | |
| — vagas — | | |
| vagas_1994 | MATCH | |
| vagas_1996 | MATCH | |
| vagas_1998 | MATCH | |
| vagas_2000 | MATCH | |
| vagas_2002 | MATCH | |
| vagas_2004 | MATCH | |
| vagas_2006 | MATCH | |
| vagas_2008 | MATCH | |
| vagas_2010 | MATCH | |
| vagas_2012 | MATCH | |
| vagas_2014 | MATCH | |
| vagas_2016 | MATCH | |
| vagas_2018 | MATCH | |
| vagas_2020 | MATCH | |
| vagas_2022 | MATCH | |
| vagas_2024 | MATCH | |
| — bens — | | |
| bens_candidato_2006 | DIFF rows 86946 vs 86725 | |
| bens_candidato_2008 | MATCH | |
| bens_candidato_2010 | DIFF rows 81226 vs 81050 | |
| bens_candidato_2012 | MATCH | |
| bens_candidato_2014 | DIFF rows 83055 vs 82837 | |
| bens_candidato_2016 | MATCH | |
| bens_candidato_2018 | DIFF rows 93527 vs 93212 | |
| bens_candidato_2020 | MATCH | |
| bens_candidato_2022 | DIFF rows 92561 vs 92321 | |
| bens_candidato_2024 | MATCH | |
| — detalhes_mun_zona — | | |
| detalhes_votacao_municipio_zona_1994 | MATCH | |
| detalhes_votacao_municipio_zona_1996 | MATCH | |
| detalhes_votacao_municipio_zona_1998 | MATCH | |
| detalhes_votacao_municipio_zona_2000 | MATCH | |
| detalhes_votacao_municipio_zona_2002 | MATCH | |
| detalhes_votacao_municipio_zona_2004 | MATCH | |
| detalhes_votacao_municipio_zona_2006 | MATCH | |
| detalhes_votacao_municipio_zona_2008 | MATCH | |
| detalhes_votacao_municipio_zona_2010 | MATCH | |
| detalhes_votacao_municipio_zona_2012 | MATCH | |
| detalhes_votacao_municipio_zona_2014 | MATCH | |
| detalhes_votacao_municipio_zona_2016 | MATCH | |
| detalhes_votacao_municipio_zona_2018 | MATCH | |
| detalhes_votacao_municipio_zona_2020 | MATCH | |
| detalhes_votacao_municipio_zona_2022 | MATCH | |
| detalhes_votacao_municipio_zona_2024 | MATCH | |
| — detalhes_secao — | | |
| detalhes_votacao_secao_1998 | MATCH | |
| detalhes_votacao_secao_2000 | MATCH | |
| detalhes_votacao_secao_2002 | MATCH | |
| detalhes_votacao_secao_2004 | MATCH | |
| detalhes_votacao_secao_2006 | MATCH | |
| detalhes_votacao_secao_2008 | MATCH | |
| detalhes_votacao_secao_2010 | MATCH | |
| detalhes_votacao_secao_2012 | MATCH | |
| detalhes_votacao_secao_2014 | MATCH | |
| detalhes_votacao_secao_2016 | MATCH | |
| detalhes_votacao_secao_2018 | MATCH | |
| detalhes_votacao_secao_2020 | MATCH | |
| detalhes_votacao_secao_2022 | MATCH | |
| detalhes_votacao_secao_2024 | MATCH | |
| — perfil_mun_zona — | | |
| perfil_eleitorado_municipio_zona_1994 | MATCH | |
| perfil_eleitorado_municipio_zona_1996 | MATCH | |
| perfil_eleitorado_municipio_zona_1998 | MATCH | |
| perfil_eleitorado_municipio_zona_2000 | MATCH | |
| perfil_eleitorado_municipio_zona_2002 | MATCH | |
| perfil_eleitorado_municipio_zona_2004 | MATCH | |
| perfil_eleitorado_municipio_zona_2006 | MATCH | |
| perfil_eleitorado_municipio_zona_2008 | MATCH | |
| perfil_eleitorado_municipio_zona_2010 | MATCH | |
| perfil_eleitorado_municipio_zona_2012 | MATCH | |
| perfil_eleitorado_municipio_zona_2014 | MATCH | |
| perfil_eleitorado_municipio_zona_2016 | MATCH | |
| perfil_eleitorado_municipio_zona_2018 | MATCH | |
| perfil_eleitorado_municipio_zona_2020 | MATCH | |
| perfil_eleitorado_municipio_zona_2022 | MATCH | |
| perfil_eleitorado_municipio_zona_2024 | MATCH | |
| — perfil_secao — | SKIP-BIG | 60M-row builds exceed 16 GB; aa_eleicao key inert on ANO_ELEICAO local vintage → byte-identical to March parquets (Gate A cell-verified). Production build in work order 05 needs streaming/bigger host |
| — local_votacao — | | |
| perfil_eleitorado_local_votacao_2010 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2012 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2014 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2016 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2018 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2020 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2022 | NO_MARCH_REF | |
| perfil_eleitorado_local_votacao_2024 | NO_MARCH_REF | |
| — rcmz — | | |
| resultados_candidato_municipio_zona_1994 | MATCH | |
| resultados_candidato_municipio_zona_1996 | MATCH | |
| resultados_candidato_municipio_zona_1998 | MATCH | |
| resultados_candidato_municipio_zona_2000 | MATCH | |
| resultados_candidato_municipio_zona_2002 | MATCH | |
| resultados_candidato_municipio_zona_2004 | MATCH | |
| resultados_candidato_municipio_zona_2006 | MATCH | |
| resultados_candidato_municipio_zona_2008 | MATCH | |
| resultados_candidato_municipio_zona_2010 | MATCH | |
| resultados_candidato_municipio_zona_2012 | MATCH | |
| resultados_candidato_municipio_zona_2014 | MATCH | |
| resultados_candidato_municipio_zona_2016 | MATCH | |
| resultados_candidato_municipio_zona_2018 | MATCH | |
| resultados_candidato_municipio_zona_2020 | MATCH | |
| resultados_candidato_municipio_zona_2022 | MATCH | |
| resultados_candidato_municipio_zona_2024 | MATCH | |
| — rpmz — | | |
| resultados_partido_municipio_zona_1994 | MATCH | |
| resultados_partido_municipio_zona_1996 | MATCH | |
| resultados_partido_municipio_zona_1998 | MATCH | |
| resultados_partido_municipio_zona_2000 | MATCH | |
| resultados_partido_municipio_zona_2002 | MATCH | |
| resultados_partido_municipio_zona_2004 | MATCH | |
| resultados_partido_municipio_zona_2006 | MATCH | |
| resultados_partido_municipio_zona_2008 | MATCH | |
| resultados_partido_municipio_zona_2010 | MATCH | |
| resultados_partido_municipio_zona_2012 | MATCH | |
| resultados_partido_municipio_zona_2014 | MATCH | |
| resultados_partido_municipio_zona_2016 | MATCH | |
| resultados_partido_municipio_zona_2018 | MATCH | |
| resultados_partido_municipio_zona_2020 | MATCH | |
| resultados_partido_municipio_zona_2022 | MATCH | |
| resultados_partido_municipio_zona_2024 | MATCH | |
| — receitas — | | |
| receitas_candidato_2002 | MATCH | |
| receitas_candidato_2004 | MATCH | |
| receitas_candidato_2006 | MATCH | |
| receitas_candidato_2008 | MATCH | |
| receitas_candidato_2010 | MATCH | |
| receitas_candidato_2012 | MATCH | |
| receitas_candidato_2014 | MATCH | |
| receitas_candidato_2016 | MATCH | |
| receitas_candidato_2018 | MATCH | |
| receitas_candidato_2020 | MATCH | |
| receitas_candidato_2022 | MATCH | |
| receitas_candidato_2024 | MATCH | |
| — despesas — | | |
| despesas_candidato_2002 | MATCH | |
| despesas_candidato_2004 | MATCH | |
| despesas_candidato_2006 | MATCH | |
| despesas_candidato_2008 | MATCH | |
| despesas_candidato_2010 | MATCH | |
| despesas_candidato_2012 | MATCH | |
| despesas_candidato_2014 | MATCH | |
| despesas_candidato_2016 | MATCH | |
| despesas_candidato_2018 | MATCH | |
| despesas_candidato_2020 | MATCH | |
| despesas_candidato_2022 | MATCH | |
| despesas_candidato_2024 | MATCH | |
| — resultados_secao — | SKIP-BIG | 60M-row builds exceed 16 GB; aa_eleicao key inert on ANO_ELEICAO local vintage → byte-identical to March parquets (Gate A cell-verified). Production build in work order 05 needs streaming/bigger host |
