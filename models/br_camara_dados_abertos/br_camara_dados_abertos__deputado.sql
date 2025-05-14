{{ config(alias="deputado", schema="br_camara_dados_abertos") }}
with
    sql as (
        select
            regexp_extract(uri, r'/([^/]+)$') as id_deputado,
            safe_cast(nome as string) nome,
            safe_cast(nomecivil as string) nome_civil,
            safe_cast(datanascimento as date) data_nascimento,
            safe_cast(datafalecimento as date) data_falecimento,
            case
                when municipionascimento = 'SAO PAULO'
                then 'São Paulo'
                when municipionascimento = 'Moji-Mirim'
                then 'Mogi Mirim'
                when municipionascimento = "São Lourenço D'Oeste"
                then 'São Lourenço do Oeste'
                when municipionascimento = "Santa Bárbara D'Oeste"
                then "Santa Bárbara d'Oeste"
                when municipionascimento = "Araióses"
                then "Araioses"
                when municipionascimento = "Cacador"
                then "Caçador"
                when municipionascimento = "Pindaré Mirim"
                then "Pindaré-Mirim"
                when municipionascimento = "Belém de São Francisco"
                then "Belém do São Francisco"
                when municipionascimento = "Sud Menucci"
                then "Sud Mennucci"
                when municipionascimento = 'Duerê'
                then "Dueré"
                when municipionascimento = 'Santana do Livramento'
                then "Sant'Ana do Livramento"
                when municipionascimento = "Herval D'Oeste"
                then "Herval d'Oeste"
                when municipionascimento = "Guaçui"
                then "Guaçuí"
                when municipionascimento = "Lençois Paulista"
                then "Lençóis Paulista"
                when municipionascimento = "Amambaí"
                then "Amambai"
                when municipionascimento = "Santo Estevão"
                then "Santo Estêvão"
                when municipionascimento = "Poxoréu"
                then "Poxoréo"
                when municipionascimento = "Trajano de Morais"
                then "Trajano de Moraes"
                else municipionascimento
            end as municipionascimento,
            safe_cast(ufnascimento as string) sigla_uf_nascimento,
            replace(
                replace(safe_cast(siglasexo as string), 'M', 'Masculino'),
                'F',
                'Feminino'
            ) sexo,
            safe_cast(idlegislaturainicial as string) id_inicial_legislatura,
            safe_cast(idlegislaturafinal as string) id_final_legislatura,
            safe_cast(urlwebsite as string) url_site,
            safe_cast(urlwebsite as string) url_rede_social,
        from {{ set_datalake_project("br_camara_dados_abertos_staging.deputado") }}
    ),
    uniao_valores as (
        select a.*, b.nome as name_id_municipio, b.id_municipio, b.sigla_uf
        from sql as a
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` as b
            on a.municipionascimento = b.nome
            and a.sigla_uf_nascimento = b.sigla_uf
    )
select
    nome,
    nome_civil,
    data_nascimento,
    data_falecimento,
    id_municipio as id_municipio_nascimento,
    sigla_uf_nascimento,
    id_deputado,
    sexo,
    id_inicial_legislatura,
    id_final_legislatura,
    url_site,
    url_rede_social,
from uniao_valores
