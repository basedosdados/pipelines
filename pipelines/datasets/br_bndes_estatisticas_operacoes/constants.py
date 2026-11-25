"""
Constantes de br_bndes_estatisticas_operacoes.

O contexto da fonte e as decisões de modelagem estão no README do conjunto.
"""

from dataclasses import dataclass
from enum import Enum

DATASET_ID = "br_bndes_estatisticas_operacoes"

PRODUTO_DIRETA_APROVACAO_TABLE_ID = "produto_direta_aprovacao"
PRODUTO_INDIRETA_APROVACAO_TABLE_ID = "produto_indireta_aprovacao"
PORTE_APROVACAO_TABLE_ID = "porte_aprovacao"
UF_APROVACAO_TABLE_ID = "uf_aprovacao"
SETOR_CNAE_APROVACAO_TABLE_ID = "setor_cnae_aprovacao"
SUBSETOR_CNAE_APROVACAO_TABLE_ID = "subsetor_cnae_aprovacao"
SETOR_BNDES_APROVACAO_TABLE_ID = "setor_bndes_aprovacao"
SUBSETOR_BNDES_APROVACAO_TABLE_ID = "subsetor_bndes_aprovacao"
EXIM_APROVACAO_TABLE_ID = "exim_aprovacao"
PRODUTO_DIRETA_APROVACAO_DOLAR_TABLE_ID = "produto_direta_aprovacao_dolar"
PRODUTO_INDIRETA_APROVACAO_DOLAR_TABLE_ID = "produto_indireta_aprovacao_dolar"
EXIM_APROVACAO_DOLAR_TABLE_ID = "exim_aprovacao_dolar"
EXIM_SETOR_CNAE_APROVACAO_DOLAR_TABLE_ID = "exim_setor_cnae_aprovacao_dolar"
EXIM_SUBSETOR_CNAE_APROVACAO_DOLAR_TABLE_ID = (
    "exim_subsetor_cnae_aprovacao_dolar"
)
PRODUTO_DIRETA_CONSULTA_TABLE_ID = "produto_direta_consulta"
PRODUTO_INDIRETA_CONSULTA_TABLE_ID = "produto_indireta_consulta"
PORTE_CONSULTA_TABLE_ID = "porte_consulta"
UF_CONSULTA_TABLE_ID = "uf_consulta"
SETOR_CNAE_CONSULTA_TABLE_ID = "setor_cnae_consulta"
SUBSETOR_CNAE_CONSULTA_TABLE_ID = "subsetor_cnae_consulta"
SETOR_BNDES_CONSULTA_TABLE_ID = "setor_bndes_consulta"
SUBSETOR_BNDES_CONSULTA_TABLE_ID = "subsetor_bndes_consulta"
EXIM_CONSULTA_TABLE_ID = "exim_consulta"
PRODUTO_DIRETA_CONSULTA_DOLAR_TABLE_ID = "produto_direta_consulta_dolar"
PRODUTO_INDIRETA_CONSULTA_DOLAR_TABLE_ID = "produto_indireta_consulta_dolar"
EXIM_CONSULTA_DOLAR_TABLE_ID = "exim_consulta_dolar"
EXIM_SETOR_CNAE_CONSULTA_DOLAR_TABLE_ID = "exim_setor_cnae_consulta_dolar"
EXIM_SUBSETOR_CNAE_CONSULTA_DOLAR_TABLE_ID = (
    "exim_subsetor_cnae_consulta_dolar"
)


PRODUTO_LABELS = {
    "bndes_finem": {"produto": "Bndes finem"},
    "bndes_exim": {"produto": "Bndes-exim"},
    "bndes_mercado_de_capitais": {"produto": "Bndes mercado de capitais"},
    "bndes_nao_reembolsavel": {"produto": "Bndes não reembolsável"},
    "bndes_microcredito": {"produto": "Bndes microcrédito"},
    "bndes_finame": {"produto": "Bndes finame"},
    "bndes_medias_empresas": {"produto": "Bndes médias empresas"},
    "bndes_automatico": {"produto": "Bndes automático"},
    "bndes_maquinas_e_servicos": {"produto": "Bndes máquinas e serviços"},
}


PORTE_LABELS = {
    "micro": {"porte_empresa": "Micro"},
    "pequena": {"porte_empresa": "Pequena"},
    "media": {"porte_empresa": "Média"},
    "grande": {"porte_empresa": "Grande"},
}


UF_LABELS = {
    "acre": {"sigla_uf": "AC"},
    "amapa": {"sigla_uf": "AP"},
    "amazonas": {"sigla_uf": "AM"},
    "para": {"sigla_uf": "PA"},
    "rondonia": {"sigla_uf": "RO"},
    "roraima": {"sigla_uf": "RR"},
    "tocantins": {"sigla_uf": "TO"},
    "alagoas": {"sigla_uf": "AL"},
    "bahia": {"sigla_uf": "BA"},
    "ceara": {"sigla_uf": "CE"},
    "maranhao": {"sigla_uf": "MA"},
    "paraiba": {"sigla_uf": "PB"},
    "pernambuco": {"sigla_uf": "PE"},
    "piaui": {"sigla_uf": "PI"},
    "rio_grande_do_norte": {"sigla_uf": "RN"},
    "sergipe": {"sigla_uf": "SE"},
    "espirito_santo": {"sigla_uf": "ES"},
    "minas_gerais": {"sigla_uf": "MG"},
    "rio_de_janeiro": {"sigla_uf": "RJ"},
    "sao_paulo": {"sigla_uf": "SP"},
    "parana": {"sigla_uf": "PR"},
    "rio_grande_do_sul": {"sigla_uf": "RS"},
    "santa_catarina": {"sigla_uf": "SC"},
    "distrito_federal": {"sigla_uf": "DF"},
    "goias": {"sigla_uf": "GO"},
    "mato_grosso": {"sigla_uf": "MT"},
    "mato_grosso_do_sul": {"sigla_uf": "MS"},
}


SETOR_CNAE_LABELS = {
    "agropecuaria": {"setor_cnae": "Agropecuária e pesca"},
    "industria_extrativa": {"setor_cnae": "Indústria extrativa"},
    "industria_de_transformacao": {"setor_cnae": "Indústria de transformação"},
    "comercio_e_servicos": {"setor_cnae": "Comércio e serviços"},
}


SUBSETOR_CNAE_LABELS = {
    "agropecuaria": {
        "setor_cnae": "Agropecuária e pesca",
        "subsetor_cnae_agrupado": "Agropecuária",
    },
    "industria_extrativa": {
        "setor_cnae": "Indústria extrativa",
        "subsetor_cnae_agrupado": "Indústria extrativa",
    },
    "bebidas": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Bebidas",
    },
    "borracha_e_plastico": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Borracha e plástico",
    },
    "celulose_e_papel": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Celulose e papel",
    },
    "confec_vestuario_e_acessorios": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Confec, vestuário e acessórios",
    },
    "coque_petroleo_e_combustivel": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Coque, petróleo e combustível",
    },
    "couro_artefato_e_calcado": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Couro, artefato e calçado",
    },
    "equip_info_eletronico_otico": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Equip info, eletrônico, ótico",
    },
    "farmoquimico_farmaceutico": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Farmoquímico, farmacêutico",
    },
    "fumo": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Fumo",
    },
    "grafica": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Gráfica",
    },
    "madeira": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Madeira",
    },
    "manutencao_reparacao_instal": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Manutenção, reparação, instal.",
    },
    "maq_aparelho_eletrico": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Máq, aparelho elétrico",
    },
    "maquinas_e_equipamentos": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Máquinas e equipamentos",
    },
    "metalurgia": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Metalurgia",
    },
    "mineral_nao_metalico": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Mineral não metálico",
    },
    "moveis": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Móveis",
    },
    "outros_equip_transporte": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Outros equip transporte",
    },
    "produto_de_metal": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Produto de metal",
    },
    "produtos_alimenticios": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Produtos alimentícios",
    },
    "produtos_diversos": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Produtos diversos",
    },
    "quimica": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Química",
    },
    "textil": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Têxtil",
    },
    "veiculo_reboque_e_carroceria": {
        "setor_cnae": "Indústria de transformação",
        "subsetor_cnae_agrupado": "Veículo, reboque e carroceria",
    },
    "administracao_publica": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Administração pública",
    },
    "agua_esgoto_e_lixo": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Água, esgoto e lixo",
    },
    "alojamento_e_alimentacao": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Alojamento e alimentação",
    },
    "artes_cultura_e_esporte": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Artes, cultura e esporte",
    },
    "ativ_aux_transporte_e_entrega": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Ativ aux transporte e entrega",
    },
    "ativ_financeira_e_seguro": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Ativ financeira e seguro",
    },
    "ativ_imobil_profissional_e_adm": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Ativ imobil, profissional e adm",
    },
    "comercio": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Comércio",
    },
    "construcao": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Construção",
    },
    "educacao": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Educação",
    },
    "eletricidade_e_gas": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Eletricidade e gás",
    },
    "informacao_e_comunicacao": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Informação e comunicação",
    },
    "organismos_internacionais": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Organismos internacionais",
    },
    "outras_ativ_servicos": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Outras ativ serviços",
    },
    "saude_e_serv_social": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Saúde e serv social",
    },
    "telecomunicacoes": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Telecomunicações",
    },
    "transporte_aereo": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Transporte aéreo",
    },
    "transporte_aquaviario": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Transporte aquaviário",
    },
    "transporte_terrestre": {
        "setor_cnae": "Comércio e serviços",
        "subsetor_cnae_agrupado": "Transporte terrestre",
    },
}


SETOR_BNDES_LABELS = {
    "agropecuaria": {"setor_bndes": "Agropecuária"},
    "comercio_e_servicos": {"setor_bndes": "Comércio e serviços"},
    "industria": {"setor_bndes": "Indústria"},
    "infraestrutura": {"setor_bndes": "Infraestrutura"},
}


SUBSETOR_BNDES_LABELS = {
    "agropecuaria_industria": {
        "setor_bndes": "Agropecuária",
        "subsetor_bndes": "Agropecuária",
    },
    "comercio_e_servicos_industria": {
        "setor_bndes": "Comércio e serviços",
        "subsetor_bndes": "Comércio e serviços",
    },
    "alimento_e_bebida_industria": {
        "setor_bndes": "Indústria",
        "subsetor_bndes": "Alimento e bebida",
    },
    "celulose_e_papel_industria": {
        "setor_bndes": "Indústria",
        "subsetor_bndes": "Celulose e papel",
    },
    "extrativa_industria": {
        "setor_bndes": "Indústria",
        "subsetor_bndes": "Extrativa",
    },
    "material_de_transporte_industria": {
        "setor_bndes": "Indústria",
        "subsetor_bndes": "Material de transporte",
    },
    "mecanica_industria": {
        "setor_bndes": "Indústria",
        "subsetor_bndes": "Mecânica",
    },
    "metalurgia_e_produtos_industria": {
        "setor_bndes": "Indústria",
        "subsetor_bndes": "Metalurgia e produtos",
    },
    "outras_industria": {
        "setor_bndes": "Indústria",
        "subsetor_bndes": "Outras",
    },
    "quimica_e_petroquimica_industria": {
        "setor_bndes": "Indústria",
        "subsetor_bndes": "Química e petroquímica",
    },
    "textil_e_vestuario_industria": {
        "setor_bndes": "Indústria",
        "subsetor_bndes": "Têxtil e vestuário",
    },
    "atv_aux_transportes_infraestrutura": {
        "setor_bndes": "Infraestrutura",
        "subsetor_bndes": "Atividades auxiliares de transportes",
    },
    "construcao_infraestrutura": {
        "setor_bndes": "Infraestrutura",
        "subsetor_bndes": "Construção",
    },
    "energia_eletrica_infraestrutura": {
        "setor_bndes": "Infraestrutura",
        "subsetor_bndes": "Energia elétrica",
    },
    "outros_infraestrutura": {
        "setor_bndes": "Infraestrutura",
        "subsetor_bndes": "Outros",
    },
    "outros_transportes_infraestrutura": {
        "setor_bndes": "Infraestrutura",
        "subsetor_bndes": "Outros transportes",
    },
    "serv_utilidade_publica_infraestrutura": {
        "setor_bndes": "Infraestrutura",
        "subsetor_bndes": "Serviços de utilidade pública",
    },
    "telecomunicacoes_infraestrutura": {
        "setor_bndes": "Infraestrutura",
        "subsetor_bndes": "Telecomunicações",
    },
    "transporte_ferroviario_infraestrutura": {
        "setor_bndes": "Infraestrutura",
        "subsetor_bndes": "Transporte ferroviário",
    },
    "transporte_rodoviario_infraestrutura": {
        "setor_bndes": "Infraestrutura",
        "subsetor_bndes": "Transporte rodoviário",
    },
}


MODALIDADE_EXIM_LABELS = {
    "bndes_exim_pre_embarque": {"modalidade_exim": "Pré-embarque"},
    "bndes_exim_pos_embarque": {"modalidade_exim": "Pós-embarque"},
}


@dataclass(frozen=True)
class TableSpec:
    """O que muda de uma tabela para outra.

    Attributes:
        resource_id: ID do recurso no CKAN do BNDES.
        category_columns: Colunas de categoria da tabela, na ordem.
        labels: Coluna da fonte -> rótulo de cada coluna de categoria.
        value_column: Nome da coluna de valor.
        factor: Multiplicador da unidade da fonte para a base 1.
    """

    resource_id: str
    category_columns: list[str]
    labels: dict[str, dict[str, str]]
    value_column: str
    factor: int


class constants(Enum):
    """Constantes de br_bndes_estatisticas_operacoes."""

    RESOURCE_SHOW_URL = (
        "https://dadosabertos.bndes.gov.br/api/3/action/resource_show"
    )
    PATH = "/tmp/br_bndes_estatisticas_operacoes/"
    SEPARATOR = ";"
    ENCODING = "cp1252"
    PERIOD_COLUMNS = ["ano", "mes"]
    PARTITION_COLUMNS = ["ano"]

    TABLES = {
        PRODUTO_DIRETA_APROVACAO_TABLE_ID: TableSpec(
            resource_id="e541b6ab-7fe3-48ca-8b43-221ce73180cc",
            category_columns=["produto"],
            labels=PRODUTO_LABELS,
            value_column="valor_aprovado",
            factor=1_000_000,
        ),
        PRODUTO_INDIRETA_APROVACAO_TABLE_ID: TableSpec(
            resource_id="0ac8adcb-5276-4132-a79d-d876763e1ee0",
            category_columns=["produto"],
            labels=PRODUTO_LABELS,
            value_column="valor_aprovado",
            factor=1_000_000,
        ),
        PORTE_APROVACAO_TABLE_ID: TableSpec(
            resource_id="34368fb3-a72c-41a4-b6b8-457461d8f513",
            category_columns=["porte_empresa"],
            labels=PORTE_LABELS,
            value_column="valor_aprovado",
            factor=1_000_000,
        ),
        UF_APROVACAO_TABLE_ID: TableSpec(
            resource_id="e41305e1-d78a-4ee4-8083-601dcd36a6b1",
            category_columns=["sigla_uf"],
            labels=UF_LABELS,
            value_column="valor_aprovado",
            factor=1_000_000,
        ),
        SETOR_CNAE_APROVACAO_TABLE_ID: TableSpec(
            resource_id="fcbaadeb-f402-42d3-b6a5-7b7d4ea588d1",
            category_columns=["setor_cnae"],
            labels=SETOR_CNAE_LABELS,
            value_column="valor_aprovado",
            factor=1_000_000,
        ),
        SUBSETOR_CNAE_APROVACAO_TABLE_ID: TableSpec(
            resource_id="b8f3d5c9-c3cc-4414-b070-445687cbc9a4",
            category_columns=["setor_cnae", "subsetor_cnae_agrupado"],
            labels=SUBSETOR_CNAE_LABELS,
            value_column="valor_aprovado",
            factor=1_000_000,
        ),
        SETOR_BNDES_APROVACAO_TABLE_ID: TableSpec(
            resource_id="0a0611b5-b834-47b6-aeb5-3d425b51a4ed",
            category_columns=["setor_bndes"],
            labels=SETOR_BNDES_LABELS,
            value_column="valor_aprovado",
            factor=1_000_000,
        ),
        SUBSETOR_BNDES_APROVACAO_TABLE_ID: TableSpec(
            resource_id="07921a0c-a262-4f17-a0ad-c277cbddc51b",
            category_columns=["setor_bndes", "subsetor_bndes"],
            labels=SUBSETOR_BNDES_LABELS,
            value_column="valor_aprovado",
            factor=1_000_000,
        ),
        EXIM_APROVACAO_TABLE_ID: TableSpec(
            resource_id="5a470cc3-ef3f-4f8d-9eee-749071146f32",
            category_columns=["modalidade_exim"],
            labels=MODALIDADE_EXIM_LABELS,
            value_column="valor_aprovado",
            factor=1_000_000,
        ),
        PRODUTO_DIRETA_APROVACAO_DOLAR_TABLE_ID: TableSpec(
            resource_id="5f14e415-e808-4ef0-a0de-2e74c5987e26",
            category_columns=["produto"],
            labels=PRODUTO_LABELS,
            value_column="valor_aprovado",
            factor=1_000,
        ),
        PRODUTO_INDIRETA_APROVACAO_DOLAR_TABLE_ID: TableSpec(
            resource_id="0dcead77-d027-445b-a64e-8725a5603a5b",
            category_columns=["produto"],
            labels=PRODUTO_LABELS,
            value_column="valor_aprovado",
            factor=1_000,
        ),
        EXIM_APROVACAO_DOLAR_TABLE_ID: TableSpec(
            resource_id="38a97485-dcb7-468b-8de3-8bd4bb9a6fc3",
            category_columns=["modalidade_exim"],
            labels=MODALIDADE_EXIM_LABELS,
            value_column="valor_aprovado",
            factor=1_000,
        ),
        EXIM_SETOR_CNAE_APROVACAO_DOLAR_TABLE_ID: TableSpec(
            resource_id="99b71a9e-b5d6-4190-9867-6a36208056f3",
            category_columns=["setor_cnae"],
            labels=SETOR_CNAE_LABELS,
            value_column="valor_aprovado",
            factor=1_000,
        ),
        EXIM_SUBSETOR_CNAE_APROVACAO_DOLAR_TABLE_ID: TableSpec(
            resource_id="f80be8b7-14b0-4659-a9b5-28d2885cd551",
            category_columns=["setor_cnae", "subsetor_cnae_agrupado"],
            labels=SUBSETOR_CNAE_LABELS,
            value_column="valor_aprovado",
            factor=1_000,
        ),
        PRODUTO_DIRETA_CONSULTA_TABLE_ID: TableSpec(
            resource_id="2a772b88-6f81-4a41-97a7-21b5e43b4b92",
            category_columns=["produto"],
            labels=PRODUTO_LABELS,
            value_column="valor_consulta",
            factor=1_000_000,
        ),
        PRODUTO_INDIRETA_CONSULTA_TABLE_ID: TableSpec(
            resource_id="63f6f9ff-dab9-4e8e-903c-41b828eca6ef",
            category_columns=["produto"],
            labels=PRODUTO_LABELS,
            value_column="valor_consulta",
            factor=1_000_000,
        ),
        PORTE_CONSULTA_TABLE_ID: TableSpec(
            resource_id="38d916d8-7464-4e96-a318-04e60087d94f",
            category_columns=["porte_empresa"],
            labels=PORTE_LABELS,
            value_column="valor_consulta",
            factor=1_000_000,
        ),
        UF_CONSULTA_TABLE_ID: TableSpec(
            resource_id="de9977d1-f23a-47e6-901f-383f59340878",
            category_columns=["sigla_uf"],
            labels=UF_LABELS,
            value_column="valor_consulta",
            factor=1_000_000,
        ),
        SETOR_CNAE_CONSULTA_TABLE_ID: TableSpec(
            resource_id="43e03959-bf33-4715-b784-e5fb840ae6c6",
            category_columns=["setor_cnae"],
            labels=SETOR_CNAE_LABELS,
            value_column="valor_consulta",
            factor=1_000_000,
        ),
        SUBSETOR_CNAE_CONSULTA_TABLE_ID: TableSpec(
            resource_id="dde28946-e1ab-40ef-9729-be1506ac0c23",
            category_columns=["setor_cnae", "subsetor_cnae_agrupado"],
            labels=SUBSETOR_CNAE_LABELS,
            value_column="valor_consulta",
            factor=1_000_000,
        ),
        SETOR_BNDES_CONSULTA_TABLE_ID: TableSpec(
            resource_id="3aeea564-b106-46ca-8965-2c6ec4c832c7",
            category_columns=["setor_bndes"],
            labels=SETOR_BNDES_LABELS,
            value_column="valor_consulta",
            factor=1_000_000,
        ),
        SUBSETOR_BNDES_CONSULTA_TABLE_ID: TableSpec(
            resource_id="a52b87b6-573f-4206-ba35-d25e3b57c5f7",
            category_columns=["setor_bndes", "subsetor_bndes"],
            labels=SUBSETOR_BNDES_LABELS,
            value_column="valor_consulta",
            factor=1_000_000,
        ),
        EXIM_CONSULTA_TABLE_ID: TableSpec(
            resource_id="a0bc5a7e-4c0e-45f2-8fec-a062712368b0",
            category_columns=["modalidade_exim"],
            labels=MODALIDADE_EXIM_LABELS,
            value_column="valor_consulta",
            factor=1_000_000,
        ),
        PRODUTO_DIRETA_CONSULTA_DOLAR_TABLE_ID: TableSpec(
            resource_id="389de879-ab97-42cf-8de4-13643eab614c",
            category_columns=["produto"],
            labels=PRODUTO_LABELS,
            value_column="valor_consulta",
            factor=1_000,
        ),
        PRODUTO_INDIRETA_CONSULTA_DOLAR_TABLE_ID: TableSpec(
            resource_id="b39d5070-8c20-4ed5-b81b-71f8198e5d8b",
            category_columns=["produto"],
            labels=PRODUTO_LABELS,
            value_column="valor_consulta",
            factor=1_000,
        ),
        EXIM_CONSULTA_DOLAR_TABLE_ID: TableSpec(
            resource_id="9f1fefc1-2b1e-48f2-97d4-aa6bfc5d4980",
            category_columns=["modalidade_exim"],
            labels=MODALIDADE_EXIM_LABELS,
            value_column="valor_consulta",
            factor=1_000,
        ),
        EXIM_SETOR_CNAE_CONSULTA_DOLAR_TABLE_ID: TableSpec(
            resource_id="8d07da0b-49af-4c19-873c-db9a4bb40d83",
            category_columns=["setor_cnae"],
            labels=SETOR_CNAE_LABELS,
            value_column="valor_consulta",
            factor=1_000,
        ),
        EXIM_SUBSETOR_CNAE_CONSULTA_DOLAR_TABLE_ID: TableSpec(
            resource_id="b95435ca-94e5-42f0-bd3e-11d4611a07c2",
            category_columns=["setor_cnae", "subsetor_cnae_agrupado"],
            labels=SUBSETOR_CNAE_LABELS,
            value_column="valor_consulta",
            factor=1_000,
        ),
    }
