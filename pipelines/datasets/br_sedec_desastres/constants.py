"""Constantes do pipeline recorrente br_sedec_desastres (Prefect 3).

Relatório "Reconhecimentos vigentes" do S2ID (Sistema Integrado de Informações
sobre Desastres), mantido pela SEDEC, vinculada ao MIDR.

O contexto da fonte e as decisões de desenho — as fechadas e as que seguem
abertas — estão em `pipelines/datasets/br_sedec_desastres/README.md`.
"""

from enum import Enum

# Cabeçalho do painel do relatório e o div de conteúdo que o segue. Servem de
# prefixo para escopar os XPaths ao painel certo: a página tem 13 painéis de
# relatório, e vários repetem os mesmos rótulos ("Todas as tipologias de
# desastres" aparece em 5). XPath sem escopo casa o primeiro, que está num painel
# fechado — e aí o Selenium espera por um elemento invisível até o timeout.
_PANEL = "//h3[contains(normalize-space(.), 'Reconhecimentos vigentes')]"
_CONTENT = f"{_PANEL}/following-sibling::div[contains(@class,'ui-accordion-content')][1]"


class constants(Enum):
    """Constantes do pipeline br_sedec_desastres.

    Nome da classe em minúsculo segue a convenção do repo para enums de
    constantes de dataset.
    """

    DATASET_ID = "br_sedec_desastres"

    TABLE_ID = "reconhecimentos_vigentes"
    ALL_TABLES = ["reconhecimentos_vigentes"]

    # Página do relatório. A fonte é uma aplicação JSF/PrimeFaces: o export sai
    # de um postback no formulário, não de uma URL de download estável — daí a
    # raspagem via browser (selenium), e não via requests.
    BASE_URL = "https://s2id.mi.gov.br/paginas/relatorios/"

    # Âncoras de texto do painel do relatório. Ainda SEM USO: são o estado final
    # da raspagem (ancorar por texto), não o atual — ver o roadmap da task.
    PANEL_TITLE = "Reconhecimentos vigentes"
    EXPORT_BUTTON_LABEL = "Exportar CSV"

    # Caminhos (XPath) dos elementos, na ordem em que são clicados. Concentrados
    # aqui porque são o acoplamento com o HTML da fonte — é o que quebra quando a
    # página muda.
    #
    # Os que dependem de rótulo são escopados por _CONTENT (ver acima); os
    # demais usam id `abas:sanfonas:*`, gerado pelo JSF e portanto sujeito a
    # trocar quando a página mudar.
    #
    # Verificados contra o HTML em 2026-08-04: cada um casa **exatamente um**
    # elemento, e todos estão dentro do painel de vigentes (os irmãos do botão
    # são j_idt145=PDF e j_idt146=XLS). Ao editar qualquer valor daqui, revalidar
    # a contagem — um XPath que casa 2+ não falha, escolhe o primeiro.
    XPATHS = {
        "painel": _PANEL,
        # Marca os 65 COBRADEs de uma vez. Ancorado no texto do <label for=...>,
        # que é estável, mas OBRIGATORIAMENTE escopado ao painel: esse mesmo
        # rótulo existe em 5 painéis, e sem o escopo o match cai no primeiro
        # ("Danos informados"), que está fechado.
        "todas_tipologias": (
            f"{_CONTENT}"
            "//label[normalize-space(.)='Todas as tipologias de desastres']"
        ),
        # O <select> nativo do estado fica dentro de um
        # div.ui-helper-hidden-accessible, ou seja, escondido — o Select() do
        # selenium levanta ElementNotInteractableException nele. A interação é:
        # clicar no widget para abrir o painel, depois clicar no <li> do estado.
        # Ler o select oculto ainda serve para pegar o par (sigla, nome).
        "estado_widget": "//*[@id='abas:sanfonas:j_idt142']",
        "estado_select_oculto": "//select[@id='abas:sanfonas:j_idt142_input']",
        "estado_item": (
            "//div[@id='abas:sanfonas:j_idt142_panel']"
            "//li[@data-label='{uf_nome}']"
        ),
        "exportar_csv": "//*[@id='abas:sanfonas:j_idt147']",
    }

    USER_AGENT = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    )

    # Prazos em segundos: espera por elemento na página, e espera pelo arquivo
    # baixado terminar de escrever.
    ELEMENT_TIMEOUT = 120
    DOWNLOAD_TIMEOUT = 300

    # Ordem, tipo e nome de origem das colunas da tabela final.
    #
    # Espelha a planilha de arquitetura, que fica em task_davi/bndes/ e não é
    # versionada junto do pipeline — por isso o schema precisa estar aqui, e não
    # lido de um CSV. Ao mexer na planilha, mexer aqui também.
    #
    # `original_name` vazio = coluna que não vem da fonte. COBRADE aparece duas
    # vezes de propósito: um campo da fonte que virá duas colunas.
    COLUNAS = [
        {
            "name": "data_extracao",
            "bigquery_type": "DATE",
            "original_name": "",
        },
        {"name": "sigla_uf", "bigquery_type": "STRING", "original_name": "UF"},
        {
            "name": "id_municipio",
            "bigquery_type": "STRING",
            "original_name": "Município",
        },
        {
            "name": "id_cobrade",
            "bigquery_type": "STRING",
            "original_name": "COBRADE",
        },
        {
            "name": "nome_cobrade",
            "bigquery_type": "STRING",
            "original_name": "COBRADE",
        },
        {
            "name": "situacao",
            "bigquery_type": "STRING",
            "original_name": "Situação",
        },
        {
            "name": "data_ocorrencia",
            "bigquery_type": "DATE",
            "original_name": "Data de Ocorrência",
        },
        {
            "name": "data_vigencia",
            "bigquery_type": "DATE",
            "original_name": "Data da Vigência",
        },
    ]

    # Onde a tabela final e o staging divergem: {coluna da arquitetura: coluna do
    # parquet}.
    #
    # `id_municipio` é produzido no modelo dbt, pelo join contra
    # `basedosdados.br_bd_diretorios_brasil.municipio` — padrão do repo, ver
    # models/br_bndes_operacoes_contratadas/
    # br_bndes_operacoes_contratadas__operacoes_administracao_publica.sql.
    # O staging carrega `nome_municipio`, que é a chave desse join e, como no
    # BNDES, não sobrevive à tabela final (o nome já vive no diretório).
    #
    # A substituição herda o `bigquery_type` da coluna da arquitetura; aqui as
    # duas são STRING, então não há perda. Se algum dia os tipos divergirem, este
    # atalho deixa de valer.
    STAGING_SUBSTITUICOES = {"id_municipio": "nome_municipio"}
