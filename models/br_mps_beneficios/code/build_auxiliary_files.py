"""Build the per-table auxiliary-file bundles for br_mps_beneficios.

One bundle per table, containing only that table's documentation, each with a
README recording the citation, per-file provenance and download date, and the
transformations applied on load — per .claude/rules/auxiliary-files.md.
"""

import shutil
import zipfile
from datetime import date
from pathlib import Path

IN = Path("/Users/rdah0003/Library/Caches/br_mps_beneficios_data/input")
OUT = Path("/Users/rdah0003/Library/Caches/br_mps_beneficios_data/auxiliary")
TODAY = date.today().isoformat()

GLOSS_URL = (
    "https://dadosabertos.inss.gov.br/dataset/"
    "glossarios-dos-arquivos-de-beneficios-plano-de-dados-abertos-jun-2023-a-jun-2025"
)

FILES = {
    "glossario_beneficios_concedidos.xlsx": (
        IN / "gloss_conc.xlsx",
        "Glossário de dados dos benefícios concedidos — descreve cada campo do "
        "extrato mensal de concessões, incluindo a definição de Mun Resid como "
        "código da Gerência-Executiva seguido da UF e do município.",
        GLOSS_URL,
    ),
    "glossario_beneficios_mantidos.xlsx": (
        IN / "gloss_mantidos.xlsx",
        "Glossário de dados dos benefícios mantidos — descreve cada campo do "
        "extrato mensal do estoque. Registra que o campo Espécie traz apenas o "
        "nome da espécie, sem o código.",
        GLOSS_URL,
    ),
    "dicionario_especies_beneficio.xlsx": (
        IN / "dic_especie.xlsx",
        "Dicionário de Dados - Espécies de Benefício — tabela oficial de "
        "códigos das espécies publicada no Plano de Dados Abertos 2025/2027.",
        "https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/"
        "PDA_2025_2027/Dicion%C3%A1rio+Dados_Esp%C3%A9cies+de+Benef%C3%ADcio_+C%C3%B3digo.xlsx",
    ),
}

BUNDLES = {
    "beneficio_concedido_municipio_mes": [
        "glossario_beneficios_concedidos.xlsx",
        "dicionario_especies_beneficio.xlsx",
    ],
    "beneficio_mantido_municipio_mes": [
        "glossario_beneficios_mantidos.xlsx",
        "dicionario_especies_beneficio.xlsx",
    ],
    "dicionario_especie": ["dicionario_especies_beneficio.xlsx"],
}

COMMON_NOTES = """
## Transformações aplicadas na carga

Estes arquivos documentam a fonte, não a tabela publicada. As diferenças entre
os dois:

- **`id_municipio` é derivado por nome.** A fonte não publica código de
  município: conforme o glossário, os dígitos de `Mun Resid` são o código da
  Gerência-Executiva (GEX) do INSS. A correspondência para o código IBGE de 7
  dígitos é feita pelo nome, com uma lista de exceções para grafias arcaicas e
  municípios renomeados (`models/br_mps_beneficios/code/municipio_crosswalk.csv`).
- **`id_municipio` e `sigla_uf` são nulos** quando a fonte registra o sentinela
  `00000-Zerada`. A proporção cresceu ao longo do tempo, de 0,05% das linhas em
  2020 a 8,04% em 2024.
- **A coluna `UF` da fonte não foi usada.** Ela se refere à Agência da
  Previdência Social concessora, não à residência do titular, e divergem.
- **`faixa_etaria` é derivada** da data de nascimento do titular contra a
  competência, em faixas equivalentes às do AEPS. A fonte não publica a idade.
- **`categoria_beneficio` é derivada** do código da espécie, e não do rótulo: a
  Emenda Constitucional 103/2019 renomeou as espécies 31 e 32 sem alterar seus
  códigos, e os extratos seguem imprimindo os nomes anteriores.
- **Códigos de espécie não reconhecidos são recusados**, não gravados: a
  competência de junho de 2024 traz um CNPJ na célula do código, e os extratos
  de 2026 trazem o fragmento "Pa", vindo da coluna vizinha Classificador PA.
"""

CONCEDIDO_NOTES = """
- **`valor_total` é derivado, não publicado.** A fonte informa a renda mensal
  inicial apenas como múltiplo do salário mínimo (`Qt SM RMI`). A conversão usa
  o salário mínimo nominal vigente na competência, incluindo a mudança de
  R$ 1.302 para R$ 1.320 em maio de 2023 (MP 1.172/2023). A medida original é
  preservada em `valor_total_salarios_minimos`. Valores nominais, sem
  deflacionamento.
- **Valores de `Qt SM RMI` fora da faixa de 0 a 200 salários mínimos são
  descartados.** Uma linha de junho de 2024 registra cerca de -2.000.000.000
  salários mínimos, o suficiente para inverter o sinal do total da série.
"""

MANTIDO_NOTES = """
- **`especie_beneficio` é nulo na maioria das linhas.** A fonte não publica o
  código da espécie em benefícios mantidos e trunca o rótulo em 20 caracteres.
  Treze prefixos truncados são compartilhados por mais de uma espécie — por
  exemplo "Aposentadoria por Id" cobre as espécies 8, 41 e 81 — de modo que o
  código só é preenchido quando o prefixo identifica uma única espécie. O
  rótulo publicado é preservado em `especie_beneficio_rotulo`, e
  `categoria_beneficio` está sempre preenchida, porque nenhum prefixo publicado
  é ambíguo quanto à categoria.
- **A tabela cobre apenas os benefícios ativos** (arquivos MANATIVOS). Os
  arquivos de cessados e suspensos não foram carregados.
- **A competência vem do nome do arquivo**, não do conteúdo: os extratos de
  benefícios mantidos não trazem coluna de competência.
- **A competência de julho de 2021 não traz a coluna `Vl MR`**, e o
  `valor_total` desse mês é nulo.
"""

EXTRA = {
    "beneficio_concedido_municipio_mes": CONCEDIDO_NOTES,
    "beneficio_mantido_municipio_mes": MANTIDO_NOTES,
    "dicionario_especie": "",
}


def readme(table: str, names: list[str]) -> str:
    lines = [
        f"# Arquivos auxiliares — `br_mps_beneficios.{table}`",
        "",
        "## Citação",
        "",
        "Brasil. Ministério da Previdência Social / Instituto Nacional do Seguro",
        "Social (INSS). Dados abertos de benefícios, extraídos do Sistema Único de",
        "Informações de Benefícios (SUIBE). Disponibilizados conforme o Decreto",
        "nº 8.777/2016 e a Lei de Acesso à Informação nº 12.527/2011.",
        "",
        "## Arquivos neste pacote",
        "",
    ]
    for n in names:
        _, desc, url = FILES[n]
        lines += [
            f"### `{n}`",
            "",
            desc,
            "",
            f"- Origem: {url}",
            f"- Baixado em: {TODAY}",
            "",
        ]
    lines += [
        "## Documentos não incluídos",
        "",
        "Os glossários dos demais grupos de dados do INSS (benefícios emitidos,",
        "benefícios indeferidos, comunicações de acidente de trabalho) tratam de",
        "tabelas que não fazem parte deste conjunto e estão disponíveis em",
        f"{GLOSS_URL}.",
        "",
        COMMON_NOTES.strip(),
        "",
        EXTRA[table].strip(),
        "",
        "## Cobertura",
        "",
        "O relatório de cobertura de municípios por ano está versionado em",
        "`models/br_mps_beneficios/code/coverage_beneficio_concedido_municipio_mes.md`",
        "no repositório `basedosdados/pipelines`.",
    ]
    return "\n".join(line for line in lines if line is not None) + "\n"


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for table, names in BUNDLES.items():
        work = OUT / table
        work.mkdir(parents=True, exist_ok=True)
        for n in names:
            src = FILES[n][0]
            if not src.exists():
                raise SystemExit(f"missing source document: {src}")
            shutil.copy2(src, work / n)
        (work / "README.md").write_text(readme(table, names), encoding="utf-8")
        zpath = OUT / table / "auxiliary_files.zip"
        with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
            z.write(work / "README.md", "README.md")
            for n in names:
                z.write(work / n, n)
        size = zpath.stat().st_size
        print(f"{table}: {zpath}  ({size:,} bytes)")
        with zipfile.ZipFile(zpath) as z:
            for i in z.infolist():
                print(f"    {i.filename}  {i.file_size:,} bytes")


if __name__ == "__main__":
    main()
