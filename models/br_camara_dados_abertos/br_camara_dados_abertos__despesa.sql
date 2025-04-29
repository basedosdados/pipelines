{{ config(alias="despesa", schema="br_camara_dados_abertos") }}
select distinct
    safe_cast(initcap(txnomeparlamentar) as string) nome_parlamentar,
    safe_cast(replace(cpf, ".0", "") as string) cpf,
    safe_cast(replace(idecadastro, ".0", "") as string) id_deputado,
    safe_cast(nulegislatura as int64) ano_legislatura,
    safe_cast(sguf as string) sigla_uf,
    safe_cast(sgpartido as string) sigla_partido,
    safe_cast(codlegislatura as string) id_legislatura,
    safe_cast(txtdescricao as string) categoria_despesa,
    safe_cast(txtdescricaoespecificacao as string) subcategoria_despesa,
    safe_cast(txtfornecedor as string) fornecedor,
    safe_cast(txtcnpjcpf as string) cnpj_cpf_fornecedor,
    safe_cast(txtnumero as string) numero_documento_fiscal,
    case
        when indtipodocumento = '0'
        then 'Nota fiscal'
        when indtipodocumento = '1'
        then 'Recibo ou outros'
        when indtipodocumento = '2'
        then 'Documento emitido no exterior'
        when indtipodocumento = '3'
        then 'Despesa do Parlasul'
        when indtipodocumento = '4'
        then 'Nota fiscal eletrônica'
        when indtipodocumento = '5'
        then 'Nota fiscal eletrônica'
        else indtipodocumento
    end as tipo_documento,
    safe_cast(numano as int64) as ano_competencia,
    safe_cast(nummes as int64) as mes_competencia,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(datemissao)), 'T')[
            offset(0)
        ] as date
    ) data_emissao,
    safe_cast(vlrdocumento as float64) valor_documento,
    safe_cast(vlrglosa as float64) valor_retido,
    safe_cast(vlrliquido as float64) valor_liquido,
    safe_cast(numparcela as int64) numero_parcela,
    safe_cast(txtpassageiro as string) nome_passageiro,
    safe_cast(txttrecho as string) descricao_passagem_aerea,
    safe_cast(datpagamentorestituicao as datetime) data_pagamento_restituicao,
    safe_cast(vlrrestituicao as float64) valor_restituicao,
    safe_cast(urldocumento as string) url_documento,
from {{ set_datalake_project("br_camara_dados_abertos_staging.despesa") }} as t
