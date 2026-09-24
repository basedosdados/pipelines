-- Minas Gerais (MG) contribution to world_wb_mides.licitacao_participante.
--
-- Ported from `code/licitacao_participante.ipynb`. A participant row is the
-- union of two populations, exactly as the notebook builds it:
-- * QUALIFIED bidders  -- habLicitacao, `habilitado` = 1
-- * WINNERS            -- the distinct winner of each homologated item
-- (homologLicitacao for licitacoes, fornDispensa for
-- dispensas), `vencedor` = 1
-- joined FULL OUTER on the participant's document, so someone who won without
-- appearing in habLicitacao is still present, and vice versa.
--
-- `classificado`, `endereco`, `cep` and `municipio_participante` are not in the
-- MG source and stay NULL, as they already are for MG in the published table.
-- `id_licitacao_bd` is the stable key, not the notebook's seq-based one.
with
    unidade_xwalk as (
        select distinct id_municipio, id_unidade_gestora, cod_unidade
        from
            (
                select id_municipio, id_unidade_gestora, cod_unidade
                from {{ set_datalake_project("world_wb_mides_staging.raw_empenho_mg") }}
                union distinct
                select id_municipio, id_unidade_gestora, cod_unidade
                from
                    {{ set_datalake_project("world_wb_mides_staging.raw_contrato_mg") }}
                union distinct
                select id_municipio, id_unidade_gestora, cod_unidade
                from
                    {{
                        set_datalake_project(
                            "world_wb_mides_staging.raw_despesa_dotacao_mg"
                        )
                    }}
            )
    ),
    -- One row per licitacao, carrying the stable key. Same expression as the
    -- licitacao state model; they must agree or participants stop linking.
    chave_licitacao as (
        select distinct
            t.seq_licitacao,
            t.id_municipio,
            t.ano,
            t.orgao,
            t.id_unidade_gestora,
            concat(
                t.orgao,
                ' ',
                ifnull(x.cod_unidade, concat('u:', t.id_unidade_gestora)),
                ' ',
                ifnull(t.num_processo, ''),
                ' ',
                ifnull(t.num_ano_processo, ''),
                ' ',
                ifnull(t.data_abert_proc_adm, ''),
                ' ',
                t.id_municipio,
                ' ',
                t.ano
            ) as id_licitacao_bd
        from {{ set_datalake_project("world_wb_mides_staging.raw_licitacao_mg") }} as t
        left join
            unidade_xwalk as x
            on t.id_municipio = x.id_municipio
            and t.id_unidade_gestora = x.id_unidade_gestora
    ),
    habilitados as (
        select distinct
            h.id_municipio,
            h.ano,
            h.orgao,
            h.seq_licitacao,
            h.num_documento as documento,
            h.nom_pessoa as razao_social
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_licitacao_participante_mg"
                )
            }} as h
    ),
    vencedores as (
        -- distinct winner per licitacao, from the homologated items
        select distinct
            id_municipio,
            ano,
            orgao,
            seq_licitacao,
            num_doc_vencedor as documento,
            nom_vencedor as razao_social
        from
            {{
                set_datalake_project(
                    "world_wb_mides_staging.raw_licitacao_homologacao_mg"
                )
            }}
        where num_doc_vencedor is not null
    ),
    participantes as (
        select
            coalesce(h.id_municipio, v.id_municipio) as id_municipio,
            coalesce(h.ano, v.ano) as ano,
            coalesce(h.orgao, v.orgao) as orgao,
            coalesce(h.seq_licitacao, v.seq_licitacao) as seq_licitacao,
            coalesce(h.documento, v.documento) as documento,
            coalesce(h.razao_social, v.razao_social) as razao_social,
            if(h.documento is not null, '1', '0') as habilitado,
            if(v.documento is not null, '1', '0') as vencedor
        from habilitados as h
        full join
            vencedores as v
            on h.id_municipio = v.id_municipio
            and h.ano = v.ano
            and h.orgao = v.orgao
            and h.seq_licitacao = v.seq_licitacao
            and h.documento = v.documento
    )
select
    safe_cast(p.ano as int64) as ano,
    'MG' as sigla_uf,
    safe_cast(p.id_municipio as string) as id_municipio,
    safe_cast(p.orgao as string) as orgao,
    safe_cast(k.id_unidade_gestora as string) as id_unidade_gestora,
    safe_cast(k.id_licitacao_bd as string) as id_licitacao_bd,
    safe_cast(p.seq_licitacao as string) as id_licitacao,
    safe_cast(null as string) as id_dispensa,
    safe_cast(p.razao_social as string) as razao_social,
    safe_cast(p.documento as string) as documento,
    safe_cast(p.habilitado as int64) as habilitado,
    safe_cast(null as int64) as classificado,
    safe_cast(p.vencedor as int64) as vencedor,
    safe_cast(null as string) as endereco,
    safe_cast(null as string) as cep,
    safe_cast(null as string) as municipio_participante,
    -- notebook: type inferred from the document's length -- 11 digits is a CPF
    -- (natural person), 14 a CNPJ (legal entity).
    case
        when length(regexp_replace(ifnull(p.documento, ''), r'[^0-9]', '')) = 11
        then 'CPF'
        when length(regexp_replace(ifnull(p.documento, ''), r'[^0-9]', '')) = 14
        then 'CNPJ'
        else null
    end as tipo
from participantes as p
left join
    chave_licitacao as k
    on p.id_municipio = k.id_municipio
    and p.ano = k.ano
    and p.orgao = k.orgao
    and p.seq_licitacao = k.seq_licitacao
