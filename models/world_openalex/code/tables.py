"""Table-level definitions for world_openalex: names, descriptions, keys, layout.

Single source for ``gen_dbt.py`` (models + schema.yml) and
``register_metadata.py`` (backend tables). Column-level definitions live in
``architecture/<table>.csv``.

Each entry: ``name`` and ``description`` as (pt, en, es); ``key`` is the column
combination unique per row; ``partition`` is the INT64 partition column and its
range, or None; ``cluster`` lists clustering columns; ``scoped`` marks the
billion-row works tables whose dbt tests run on recent publication years only.
"""

from typing import TypedDict


class TableSpec(TypedDict):
    """One table's definition."""

    name: tuple[str, str, str]
    description: tuple[str, str, str]
    key: list[str]
    partition: tuple[str, tuple[int, int]] | None
    cluster: list[str]
    scoped: bool


WORK_YEARS = (1500, 2031)
AUTHOR_YEARS = (1900, 2031)

SOURCE_NOTE = (
    "Fonte: snapshot do OpenAlex (OurResearch), licença CC0.",
    "Source: OpenAlex snapshot (OurResearch), CC0 license.",
    "Fuente: snapshot de OpenAlex (OurResearch), licencia CC0.",
)


def _t(
    name: tuple[str, str, str],
    description: tuple[str, str, str],
    key: list[str],
    partition: tuple[str, tuple[int, int]] | None = None,
    cluster: list[str] | None = None,
    scoped: bool = False,
) -> TableSpec:
    pt, en, es = (
        f"{d} {s}" for d, s in zip(description, SOURCE_NOTE, strict=True)
    )
    return {
        "name": name,
        "description": (pt, en, es),
        "key": key,
        "partition": partition,
        "cluster": cluster or key[:1],
        "scoped": scoped,
    }


def _w(
    name: tuple[str, str, str],
    description: tuple[str, str, str],
    key: list[str],
) -> TableSpec:
    """A works-family table: partitioned by publication year, tests scoped."""
    return _t(
        name,
        description,
        key,
        ("publication_year", WORK_YEARS),
        ["work_id"],
        True,
    )


TABLES: dict[str, TableSpec] = {
    "work": _w(
        ("Trabalhos", "Works", "Trabajos"),
        (
            "Um registro por trabalho acadêmico indexado pelo OpenAlex (artigos, livros, capítulos, teses, conjuntos de dados, preprints), com identificadores, data e tipo de publicação, fonte e tópico principais, situação de acesso aberto e contagens de autores, referências e citações.",
            "One record per scholarly work indexed by OpenAlex (articles, books, chapters, theses, datasets, preprints), with identifiers, publication date and type, primary source and topic, open access status, and counts of authors, references and citations.",
            "Un registro por trabajo académico indexado por OpenAlex (artículos, libros, capítulos, tesis, conjuntos de datos, preprints), con identificadores, fecha y tipo de publicación, fuente y tópico principales, situación de acceso abierto y conteos de autores, referencias y citas.",
        ),
        ["work_id"],
    ),
    "work_abstract": _w(
        (
            "Resumos dos trabalhos",
            "Work abstracts",
            "Resúmenes de los trabajos",
        ),
        (
            "Resumo de cada trabalho em texto corrido, reconstruído a partir do índice invertido publicado pelo OpenAlex. Cerca de metade dos trabalhos tem resumo.",
            "Abstract of each work as plain text, rebuilt from the inverted index OpenAlex publishes. About half of all works have an abstract.",
            "Resumen de cada trabajo en texto corrido, reconstruido a partir del índice invertido que publica OpenAlex. Cerca de la mitad de los trabajos tiene resumen.",
        ),
        ["work_id"],
    ),
    "work_authorship": _w(
        ("Autorias", "Authorships", "Autorías"),
        (
            "Um registro por autor de cada trabalho, na ordem da lista de autoria, com o autor desambiguado pelo OpenAlex, o nome como impresso e a indicação de autor correspondente.",
            "One record per author of each work, in authorship order, with the author as disambiguated by OpenAlex, the name as printed, and whether the author is the corresponding author.",
            "Un registro por autor de cada trabajo, en el orden de la lista de autoría, con el autor desambiguado por OpenAlex, el nombre tal como aparece impreso y si es el autor de correspondencia.",
        ),
        ["work_id", "author_sequence"],
    ),
    "work_authorship_institution": _w(
        (
            "Instituições das autorias",
            "Authorship institutions",
            "Instituciones de las autorías",
        ),
        (
            "Um registro por instituição do OpenAlex associada a cada autor de cada trabalho.",
            "One record per OpenAlex institution associated with each author of each work.",
            "Un registro por institución de OpenAlex asociada a cada autor de cada trabajo.",
        ),
        ["work_id", "author_sequence", "institution_id"],
    ),
    "work_authorship_affiliation": _w(
        (
            "Afiliações declaradas das autorias",
            "Raw authorship affiliations",
            "Afiliaciones declaradas de las autorías",
        ),
        (
            "Um registro por texto de afiliação de cada autor, como impresso no trabalho, com as instituições do OpenAlex identificadas nesse texto.",
            "One record per affiliation text of each author, as printed on the work, with the OpenAlex institutions matched to that text.",
            "Un registro por texto de afiliación de cada autor, tal como aparece impreso en el trabajo, con las instituciones de OpenAlex identificadas en ese texto.",
        ),
        ["work_id", "author_sequence", "affiliation_sequence"],
    ),
    "work_authorship_country": _w(
        (
            "Países das autorias",
            "Authorship countries",
            "Países de las autorías",
        ),
        (
            "Um registro por país de afiliação de cada autor de cada trabalho, inclusive quando a instituição não foi identificada.",
            "One record per country of affiliation of each author of each work, including when the institution was not identified.",
            "Un registro por país de afiliación de cada autor de cada trabajo, incluso cuando la institución no fue identificada.",
        ),
        ["work_id", "author_sequence", "country_code"],
    ),
    "work_location": _w(
        (
            "Locais dos trabalhos",
            "Work locations",
            "Ubicaciones de los trabajos",
        ),
        (
            "Um registro por local em que um trabalho está hospedado (periódico, repositório, servidor de preprints), com versão, licença, URLs e situação de acesso aberto, marcando o local principal e o melhor local de acesso aberto.",
            "One record per location hosting a work (journal, repository, preprint server), with version, license, URLs and open access status, flagging the primary location and the best open access location.",
            "Un registro por ubicación que aloja un trabajo (revista, repositorio, servidor de preprints), con versión, licencia, URLs y situación de acceso abierto, marcando la ubicación principal y la mejor ubicación de acceso abierto.",
        ),
        ["work_id", "location_sequence"],
    ),
    "work_topic": _w(
        ("Tópicos dos trabalhos", "Work topics", "Tópicos de los trabajos"),
        (
            "Até três tópicos atribuídos a cada trabalho pelo classificador do OpenAlex, com o escore de confiança; o tópico de ordem 1 é o principal.",
            "Up to three topics assigned to each work by the OpenAlex classifier, with the confidence score; the topic ranked 1 is the primary topic.",
            "Hasta tres tópicos asignados a cada trabajo por el clasificador de OpenAlex, con el puntaje de confianza; el tópico de orden 1 es el principal.",
        ),
        ["work_id", "topic_id"],
    ),
    "work_keyword": _w(
        (
            "Palavras-chave dos trabalhos",
            "Work keywords",
            "Palabras clave de los trabajos",
        ),
        (
            "Palavras-chave atribuídas a cada trabalho pelo OpenAlex, com o escore de confiança.",
            "Keywords assigned to each work by OpenAlex, with the confidence score.",
            "Palabras clave asignadas a cada trabajo por OpenAlex, con el puntaje de confianza.",
        ),
        ["work_id", "keyword_id"],
    ),
    "work_sdg": _w(
        ("ODS dos trabalhos", "Work SDGs", "ODS de los trabajos"),
        (
            "Objetivos de Desenvolvimento Sustentável da ONU associados a cada trabalho pelo classificador do OpenAlex, com o escore de confiança.",
            "UN Sustainable Development Goals associated with each work by the OpenAlex classifier, with the confidence score.",
            "Objetivos de Desarrollo Sostenible de la ONU asociados a cada trabajo por el clasificador de OpenAlex, con el puntaje de confianza.",
        ),
        ["work_id", "sdg_id"],
    ),
    "work_mesh": _w(
        (
            "Descritores MeSH dos trabalhos",
            "Work MeSH terms",
            "Descriptores MeSH de los trabajos",
        ),
        (
            "Descritores e qualificadores do vocabulário MeSH atribuídos pelo PubMed a cada trabalho nele indexado.",
            "MeSH descriptors and qualifiers assigned by PubMed to each work it indexes.",
            "Descriptores y calificadores del vocabulario MeSH asignados por PubMed a cada trabajo que indexa.",
        ),
        ["work_id", "descriptor_id", "qualifier_id"],
    ),
    "work_award": _w(
        (
            "Financiamentos dos trabalhos",
            "Work awards",
            "Subvenciones de los trabajos",
        ),
        (
            "Um registro por financiamento (award) declarado em cada trabalho, com o financiador e o código atribuído por ele.",
            "One record per award acknowledged by each work, with the funder and the code it assigned.",
            "Un registro por subvención declarada en cada trabajo, con el financiador y el código que asignó.",
        ),
        ["work_id", "award_id"],
    ),
    "work_funder": _w(
        (
            "Financiadores dos trabalhos",
            "Work funders",
            "Financiadores de los trabajos",
        ),
        (
            "Um registro por financiador declarado em cada trabalho.",
            "One record per funder acknowledged by each work.",
            "Un registro por financiador declarado en cada trabajo.",
        ),
        ["work_id", "funder_id"],
    ),
    "work_reference": _w(
        (
            "Referências dos trabalhos",
            "Work references",
            "Referencias de los trabajos",
        ),
        (
            "Rede de citações: um registro por trabalho citado na lista de referências de cada trabalho, quando o citado também está no OpenAlex.",
            "Citation network: one record per work cited in each work's reference list, when the cited work is also in OpenAlex.",
            "Red de citas: un registro por trabajo citado en la lista de referencias de cada trabajo, cuando el citado también está en OpenAlex.",
        ),
        ["work_id", "referenced_work_id"],
    ),
    "work_counts_by_year": _w(
        (
            "Citações dos trabalhos por ano",
            "Work citations by year",
            "Citas de los trabajos por año",
        ),
        (
            "Citações recebidas por cada trabalho em cada ano, nos últimos dez anos.",
            "Citations received by each work in each year, over the last ten years.",
            "Citas recibidas por cada trabajo en cada año, en los últimos diez años.",
        ),
        ["work_id", "year"],
    ),
    "work_indexed_in": _w(
        ("Índices dos trabalhos", "Work indexes", "Índices de los trabajos"),
        (
            "Bases e índices em que cada trabalho está indexado (Crossref, PubMed, DOAJ, arXiv, DataCite).",
            "Databases and indexes each work is indexed in (Crossref, PubMed, DOAJ, arXiv, DataCite).",
            "Bases e índices en que cada trabajo está indexado (Crossref, PubMed, DOAJ, arXiv, DataCite).",
        ),
        ["work_id", "index_name"],
    ),
    "author": _t(
        ("Autores", "Authors", "Autores"),
        (
            "Um registro por autor desambiguado pelo OpenAlex, com nome, ORCID, contagens de trabalhos e citações e índices h e i10.",
            "One record per author as disambiguated by OpenAlex, with name, ORCID, counts of works and citations, and h- and i10-indexes.",
            "Un registro por autor desambiguado por OpenAlex, con nombre, ORCID, conteos de trabajos y citas e índices h e i10.",
        ),
        ["author_id"],
    ),
    "author_alternative_name": _t(
        (
            "Nomes alternativos dos autores",
            "Author alternative names",
            "Nombres alternativos de los autores",
        ),
        (
            "Grafias alternativas do nome de cada autor encontradas pelo OpenAlex.",
            "Alternative spellings of each author's name found by OpenAlex.",
            "Grafías alternativas del nombre de cada autor encontradas por OpenAlex.",
        ),
        ["author_id", "alternative_name"],
    ),
    "author_affiliation": _t(
        (
            "Afiliações dos autores por ano",
            "Author affiliations by year",
            "Afiliaciones de los autores por año",
        ),
        (
            "Um registro por autor, instituição e ano em que o autor publicou com aquela afiliação.",
            "One record per author, institution and year in which the author published with that affiliation.",
            "Un registro por autor, institución y año en que el autor publicó con esa afiliación.",
        ),
        ["author_id", "institution_id", "year"],
        partition=("year", AUTHOR_YEARS),
        cluster=["author_id"],
    ),
    "author_last_known_institution": _t(
        (
            "Últimas instituições dos autores",
            "Author last known institutions",
            "Últimas instituciones de los autores",
        ),
        (
            "Instituições da afiliação mais recente de cada autor.",
            "Institutions of each author's most recent affiliation.",
            "Instituciones de la afiliación más reciente de cada autor.",
        ),
        ["author_id", "institution_id"],
    ),
    "author_topic": _t(
        ("Tópicos dos autores", "Author topics", "Tópicos de los autores"),
        (
            "Tópicos em que cada autor publica, com o número de trabalhos e a participação de cada tópico na produção do autor.",
            "Topics each author publishes in, with the number of works and each topic's share of the author's output.",
            "Tópicos en que publica cada autor, con el número de trabajos y la participación de cada tópico en su producción.",
        ),
        ["author_id", "topic_id"],
    ),
    "author_counts_by_year": _t(
        (
            "Produção dos autores por ano",
            "Author output by year",
            "Producción de los autores por año",
        ),
        (
            "Trabalhos publicados, trabalhos em acesso aberto e citações recebidas por cada autor em cada ano, nos últimos dez anos.",
            "Works published, open access works and citations received by each author in each year, over the last ten years.",
            "Trabajos publicados, trabajos en acceso abierto y citas recibidas por cada autor en cada año, en los últimos diez años.",
        ),
        ["author_id", "year"],
        partition=("year", AUTHOR_YEARS),
        cluster=["author_id"],
    ),
    "award": _t(
        ("Financiamentos", "Awards", "Subvenciones"),
        (
            "Um registro por financiamento (bolsa, auxílio, projeto) registrado pelo OpenAlex, com financiador, valor, moeda, datas e tópico principal.",
            "One record per award (grant, fellowship, project) recorded by OpenAlex, with funder, amount, currency, dates and primary topic.",
            "Un registro por subvención (beca, ayuda, proyecto) registrada por OpenAlex, con financiador, monto, moneda, fechas y tópico principal.",
        ),
        ["award_id"],
    ),
    "award_investigator": _t(
        (
            "Pesquisadores dos financiamentos",
            "Award investigators",
            "Investigadores de las subvenciones",
        ),
        (
            "Pesquisadores de cada financiamento, com o papel (responsável, co-responsável ou membro) e a afiliação declarada.",
            "Investigators on each award, with their role (lead, co-lead or member) and stated affiliation.",
            "Investigadores de cada subvención, con su rol (responsable, corresponsable o miembro) y la afiliación declarada.",
        ),
        ["award_id", "investigator_sequence"],
    ),
    "award_institution": _t(
        (
            "Instituições dos financiamentos",
            "Award institutions",
            "Instituciones de las subvenciones",
        ),
        (
            "Instituições do OpenAlex que receberam cada financiamento.",
            "OpenAlex institutions that received each award.",
            "Instituciones de OpenAlex que recibieron cada subvención.",
        ),
        ["award_id", "institution_id"],
    ),
    "award_topic": _t(
        (
            "Tópicos dos financiamentos",
            "Award topics",
            "Tópicos de las subvenciones",
        ),
        (
            "Tópicos atribuídos a cada financiamento pelo classificador do OpenAlex, com o escore de confiança.",
            "Topics assigned to each award by the OpenAlex classifier, with the confidence score.",
            "Tópicos asignados a cada subvención por el clasificador de OpenAlex, con el puntaje de confianza.",
        ),
        ["award_id", "topic_id"],
    ),
    "institution": _t(
        ("Instituições", "Institutions", "Instituciones"),
        (
            "Um registro por instituição (universidade, hospital, empresa, órgão público) do OpenAlex, ligada ao Research Organization Registry (ROR), com localização, tipo e contagens de trabalhos e citações.",
            "One record per OpenAlex institution (university, hospital, company, government body), linked to the Research Organization Registry (ROR), with location, type and counts of works and citations.",
            "Un registro por institución (universidad, hospital, empresa, organismo público) de OpenAlex, vinculada al Research Organization Registry (ROR), con ubicación, tipo y conteos de trabajos y citas.",
        ),
        ["institution_id"],
    ),
    "institution_association": _t(
        (
            "Associações entre instituições",
            "Institution associations",
            "Asociaciones entre instituciones",
        ),
        (
            "Relações entre instituições registradas no ROR: instituição-mãe, filha ou relacionada.",
            "Relationships between institutions recorded in ROR: parent, child or related.",
            "Relaciones entre instituciones registradas en ROR: institución matriz, hija o relacionada.",
        ),
        ["institution_id", "associated_institution_id", "relationship"],
    ),
    "source": _t(
        ("Fontes", "Sources", "Fuentes"),
        (
            "Um registro por fonte que hospeda trabalhos (periódico, repositório, conferência, série de livros), com ISSN, editora, indicadores de acesso aberto, taxa de publicação e contagens de trabalhos e citações.",
            "One record per source hosting works (journal, repository, conference, book series), with ISSN, publisher, open access indicators, publication charge and counts of works and citations.",
            "Un registro por fuente que aloja trabajos (revista, repositorio, conferencia, serie de libros), con ISSN, editorial, indicadores de acceso abierto, cargo de publicación y conteos de trabajos y citas.",
        ),
        ["source_id"],
    ),
    "source_issn": _t(
        ("ISSN das fontes", "Source ISSNs", "ISSN de las fuentes"),
        (
            "Todos os ISSN (impresso e eletrônico) de cada fonte.",
            "Every ISSN (print and electronic) of each source.",
            "Todos los ISSN (impreso y electrónico) de cada fuente.",
        ),
        ["source_id", "issn"],
    ),
    "publisher": _t(
        ("Editoras", "Publishers", "Editoriales"),
        (
            "Um registro por editora, com sua posição na hierarquia de editoras, países e contagens de trabalhos e citações.",
            "One record per publisher, with its place in the publisher hierarchy, countries and counts of works and citations.",
            "Un registro por editorial, con su posición en la jerarquía de editoriales, países y conteos de trabajos y citas.",
        ),
        ["publisher_id"],
    ),
    "funder": _t(
        ("Financiadores", "Funders", "Financiadores"),
        (
            "Um registro por organização financiadora de pesquisa, ligada ao Crossref Funder Registry e ao ROR, com contagens de trabalhos, citações e financiamentos.",
            "One record per research funding organization, linked to the Crossref Funder Registry and ROR, with counts of works, citations and awards.",
            "Un registro por organización financiadora de investigación, vinculada al Crossref Funder Registry y a ROR, con conteos de trabajos, citas y subvenciones.",
        ),
        ["funder_id"],
    ),
    "topic": _t(
        ("Tópicos", "Topics", "Tópicos"),
        (
            "Os cerca de 4.500 tópicos da classificação do OpenAlex, com descrição, palavras-chave e a subárea, área e domínio a que pertencem.",
            "The roughly 4,500 topics of the OpenAlex classification, with description, keywords, and the subfield, field and domain they belong to.",
            "Los cerca de 4.500 tópicos de la clasificación de OpenAlex, con descripción, palabras clave y la subárea, área y dominio a los que pertenecen.",
        ),
        ["topic_id"],
    ),
    "subfield": _t(
        ("Subáreas", "Subfields", "Subáreas"),
        (
            "As 252 subáreas da classificação do OpenAlex, que seguem a classificação de periódicos da Scopus (ASJC).",
            "The 252 subfields of the OpenAlex classification, which follow the Scopus journal classification (ASJC).",
            "Las 252 subáreas de la clasificación de OpenAlex, que siguen la clasificación de revistas de Scopus (ASJC).",
        ),
        ["subfield_id"],
    ),
    "field": _t(
        ("Áreas", "Fields", "Áreas"),
        (
            "As 26 áreas da classificação do OpenAlex.",
            "The 26 fields of the OpenAlex classification.",
            "Las 26 áreas de la clasificación de OpenAlex.",
        ),
        ["field_id"],
    ),
    "domain": _t(
        ("Domínios", "Domains", "Dominios"),
        (
            "Os 4 domínios da classificação do OpenAlex: ciências da vida, sociais, físicas e da saúde.",
            "The 4 domains of the OpenAlex classification: life, social, physical and health sciences.",
            "Los 4 dominios de la clasificación de OpenAlex: ciencias de la vida, sociales, físicas y de la salud.",
        ),
        ["domain_id"],
    ),
    "keyword": _t(
        ("Palavras-chave", "Keywords", "Palabras clave"),
        (
            "Vocabulário de palavras-chave que o OpenAlex atribui aos trabalhos.",
            "Vocabulary of keywords OpenAlex assigns to works.",
            "Vocabulario de palabras clave que OpenAlex asigna a los trabajos.",
        ),
        ["keyword_id"],
    ),
    "dicionario": _t(
        ("Dicionário", "Dictionary", "Diccionario"),
        (
            "Rótulos dos códigos de idioma, licença e Objetivo de Desenvolvimento Sustentável usados nas demais tabelas.",
            "Labels of the language, license and Sustainable Development Goal codes used in the other tables.",
            "Etiquetas de los códigos de idioma, licencia y Objetivo de Desarrollo Sostenible usados en las demás tablas.",
        ),
        ["id_tabela", "nome_coluna", "chave"],
    ),
}
