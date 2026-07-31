"""Acrescenta ao dicionário do br_ms_cnes os códigos novos do DATASUS.

O dicionário deste conjunto não é gerado por crawler nem por flow: é um CSV
único no staging, mantido à mão. Sempre que o DATASUS cria um código novo, o
teste `custom_dictionary_coverage` quebra e o flow morre depois de
materializar. Este script existe para que a próxima vez seja um diff
revisável.

Uso:
    export BD_SERVICE_ACCOUNT_DEV="$HOME/.basedosdados/credentials/credentials-dev.json"
    cd models/br_ms_cnes/code

    # Conferir o que será acrescentado (não altera nada)
    uv run python update_dicionario.py --dry-run

    # Aplicar: grava o CSV e sobe para o staging dev
    uv run python update_dicionario.py --apply
"""

from __future__ import annotations

import argparse
from pathlib import Path

import basedosdados as bd
import pandas as pd

DATASET_ID = "br_ms_cnes"
TABLE_ID = "dicionario"
BILLING_PROJECT = "basedosdados-dev"
STAGING_TABLE = f"{BILLING_PROJECT}.{DATASET_ID}_staging.{TABLE_ID}"

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_CSV = OUTPUT_DIR / "dicionario.csv"

COLUMNS = [
    "id_tabela",
    "nome_coluna",
    "chave",
    "cobertura_temporal",
    "valor",
]

# Todas as 1.591 linhas do dicionário usam "(1)" neste campo. Mantemos por
# consistência, embora a notação correta fosse "INICIO(1)FIM".
COBERTURA = "(1)"

# Códigos criados pelo DATASUS e ausentes do dicionário. Procedência por linha:
#
#   12, 13  Portaria SAES/MS nº 3.695, de 15/01/2026, Art. 2º §1º — primária
#   11, 14, 15, 16
#           Portaria SAES/MS nº 4.109/2026, citada por terceiros; não
#           localizamos o texto oficial (ver issue #1714)
#   82      CNES 4.8.40, divulgado por terceiros; idem
#
# As tabelas auxiliares oficiais (TAB_CNES.zip, regeradas em 21/07/2026) ainda
# trazem só 10 tipos de equipamento e param no 76 em EQUIPE.dbf — não servem.
NEW_ROWS: list[dict[str, str]] = [
    {
        "id_tabela": "equipamento",
        "nome_coluna": "tipo_equipamento",
        "chave": "11",
        "valor": "Avaliação Antropométrica e Funcional",
    },
    {
        "id_tabela": "equipamento",
        "nome_coluna": "tipo_equipamento",
        "chave": "12",
        "valor": "Radioterapia",
    },
    {
        "id_tabela": "equipamento",
        "nome_coluna": "tipo_equipamento",
        "chave": "13",
        "valor": "Quimioterapia",
    },
    {
        "id_tabela": "equipamento",
        "nome_coluna": "tipo_equipamento",
        "chave": "14",
        "valor": "Reabilitação",
    },
    {
        "id_tabela": "equipamento",
        "nome_coluna": "tipo_equipamento",
        "chave": "15",
        "valor": "Procedimentos Clínicos",
    },
    {
        "id_tabela": "equipamento",
        "nome_coluna": "tipo_equipamento",
        "chave": "16",
        "valor": "Procedimentos Cirúrgicos",
    },
    {
        "id_tabela": "equipe",
        "nome_coluna": "tipo_equipe",
        "chave": "82",
        "valor": "E-DOT - Equipe Hospitalar de Doação para Transplantes",
    },
    # Os dois indicadores do equipamento passam a ser STRING cobertos por
    # dicionário (issue #1722). O Equipamento.def do TAB_CNES documenta: "o campo
    # IND_SUS é o Indicador de Disponibilidade para o SUS e não contém
    # quantidades, mas somente 1=SIM ou 0=NÃO". Na série inteira (163,5M linhas)
    # só existem '0' e '1', e as duas colunas são exatamente complementares.
    {
        "id_tabela": "equipamento",
        "nome_coluna": "indicador_equipamento_disponivel_sus",
        "chave": "0",
        "valor": "Não",
    },
    {
        "id_tabela": "equipamento",
        "nome_coluna": "indicador_equipamento_disponivel_sus",
        "chave": "1",
        "valor": "Sim",
    },
    {
        "id_tabela": "equipamento",
        "nome_coluna": "indicador_equipamento_indisponivel_sus",
        "chave": "0",
        "valor": "Não",
    },
    {
        "id_tabela": "equipamento",
        "nome_coluna": "indicador_equipamento_indisponivel_sus",
        "chave": "1",
        "valor": "Sim",
    },
]

# Rótulos do código de 4 dígitos do equipamento (2 do tipo + 2 do equipamento).
#
# O `id_equipamento` sozinho não identifica nada: a numeração recomeça a cada
# tipo, então o equipamento 1 é "gama camara" na radiologia, "camera para
# reconhecimento facial" na telessaúde e "acelerador linear" na radioterapia.
# Só o par identifica. Ver "O código de 4 dígitos" no README do conjunto.
#
# A série tem 207 códigos distintos; os 141 abaixo têm rótulo. Procedência por
# bloco, em ordem de precedência:
#
#   Portaria 3.695  Anexo I da Portaria SAES/MS nº 3.695, de 15/01/2026
#                   (DOU de 18/05/2026, ed. 91, seção 1, p. 174). Tem
#                   precedência por ser a norma e ser mais recente que o CNV:
#                   criou 27 códigos que o CNV não traz e renomeou outros 3.
#                   O `WebFetch` não lê o PDF; `pdftotext -layout` resolve.
#   CNV             `CNV/Equip_Tp.cnv` do `TAB_CNES.zip`, versão de 21/07/2026
#                   (ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Auxiliar).
#                   É o dicionário oficial do DATASUS para este campo, em
#                   latin-1. O `curl` com ftp:// estoura timeout; `ftplib` em
#                   modo passivo funciona.
#   Legado          Transposto do próprio dicionário, que registrava o rótulo
#                   sob o número de 2 dígitos. Regra: o rótulo vai para o tipo
#                   dominante daquele número na série.
#
# Os 66 códigos restantes ficam sem rótulo de propósito: os tipos 11, 14, 15 e
# 16 dependem da Portaria 4.109, cujo texto oficial não foi localizado, e 29
# códigos antigos não têm correspondência. Por isso o
# `custom_dictionary_coverage` NÃO aponta para `codigo_equipamento` — com
# `severity: error`, derrubaria o flow todo mês.
CODIGO_EQUIPAMENTO: dict[str, str] = {
    # --- Portaria SAES/MS nº 3.695/2026, Anexo I ---
    "0103": "Mamógrafo com Estereotaxia",
    "0107": "Raio X Odontológico",
    "0117": "Mamógrafo Digital",
    "0119": "Mamógrafo com Tomossíntese",
    "0120": "Raio X Analogico",
    "0121": "Raio X Digital",
    "0122": "Raio X Telecomandado",
    "0123": "Raio X Móvel",
    "0124": "Arco-C",
    "0125": "Raio X Panorâmico",
    "0126": "Tomógrafo Computadorizado 4 Canais",
    "0127": "Tomógrafo Computadorizado 16 Canais",
    "0128": "Tomógrafo Computadorizado 32 Canais",
    "0129": "Tomógrafo Computadorizado 64 Canais",
    "0130": "Tomógrafo Computadorizado 128 Canais",
    "0131": "Tomógrafo Simulador para Radioterapia (uso exclusivo)",
    "0132": "Ressonância Magnética 0.5T",
    "0133": "Ressonância Magnética 1.5T",
    "0134": "Ressonância Magnética 3T",
    "0135": "Ressonância Magnética de Campo Aberto",
    "1201": "Acelerador Linear sem elétrons (Básico - Intermediário)",
    "1202": "Acelerador Linear com elétrons (Recursos avançados com IGRT 3D)",
    "1203": "Unidade de Cobaltoterapia",
    "1204": "Braquiterapia",
    "1205": "Sistema de Planejamento",
    "1206": "Sistema de Dosimetria",
    "1207": "Fonte SR90 selada",
    "1301": "Cabine de Segurança Biológica Classe II B2",
    "1302": "Poltrona para administração de quimioterapia",
    "1303": "Cama Hospitalar para administração de quimioterapia",
    # --- CNV/Equip_Tp.cnv, versão de 21/07/2026 ---
    "0101": "Gama Câmara",
    "0102": "Mamógrafo com Comando Simples",
    "0104": "Raio X até 100 mA",
    "0105": "Raio X de 100 a 500 mA",
    "0106": "Raio X mais de 500mA",
    "0108": "Raio X com Fluoroscopia",
    "0109": "Raio X para Densitometria Óssea",
    "0110": "Raio X para Hemodinâmica",
    "0111": "Tomógrafo Computadorizado",
    "0112": "Ressonância Magnética",
    "0113": "Ultrassom Doppler Colorido",
    "0114": "Ultrassom Ecógrafo",
    "0115": "Ultrassom Convencional",
    "0116": "Processadora de filme exclusiva para mamografia",
    "0118": "PET/CT",
    "0221": "Controle Ambiental/Ar-condicionado Central",
    "0222": "Grupo Gerador",
    "0223": "Usina de Oxigênio",
    "0331": "Endoscópio das Vias Respiratórias",
    "0332": "Endoscópio das Vias Urinárias",
    "0333": "Endoscópio Digestivo",
    "0334": "Equipamentos para Optometria",
    "0335": "Laparoscópio/Vídeo",
    "0336": "Microcópio Cirúrgico",
    "0337": "Cadeira Oftalmológica",
    "0338": "Coluna Otalmológica",
    "0339": "Refrator",
    "0340": "Lensomêtro",
    "0344": "Projetor ou Tabela de Optotipos",
    "0345": "Retinoscópio",
    "0346": "Oftalmoscópio",
    "0347": "Ceratômetro",
    "0348": "Tonômetro de Aplanação",
    "0349": "Biomicroscópio (Lâmpada de Fenda)",
    "0350": "Campímetro",
    "0441": "Eletrocardiógrafo",
    "0442": "Eletroencefalógrafo",
    "0551": "Bomba/Balão Intra-Aórtico",
    "0552": "Bomba de Infusão",
    "0553": "Berço Aquecido",
    "0554": "Bilirrubinômetro",
    "0555": "Debitômetro",
    "0556": "Desfibrilador",
    "0557": "Equipamento de Fototerapia",
    "0558": "Incubadora",
    "0559": "Marcapasso Temporário",
    "0560": "Monitor de ECG",
    "0561": "Monitor de Pressão Invasivo",
    "0562": "Monitor de Pressão Não-Invasivo",
    "0563": "Reanimador Pulmonar/AMBU",
    "0564": "Respirador/Ventilador",
    "0671": "Aparelho de Diatermia por Ultrassom/Ondas Curtas",
    "0672": "Aparelho de Eletroestimulação",
    "0673": "Bomba de Infusão de Hemoderivados",
    "0674": "Equipamentos de Aférese",
    "0675": "Equipamento para Audiometria",
    "0676": "Equipamento de Circulação Extracorpórea",
    "0677": "Equipamento para Hemodiálise",
    "0678": "Forno de Bier",
    "0780": "Equipo Odontológico Completo",
    "0781": "Compressor Odontológico",
    "0782": "Fotopolimerizador",
    "0783": "Caneta de Alta Rotação",
    "0784": "Caneta de Baixa Rotação",
    "0785": "Amalgamador",
    "0786": "Aparelho de Profilaxia c/Jato de Bicarbonato",
    "0887": "Emissões Otoacusticas Evocadas Transientes",
    "0888": "Emissões Otoacusticas Evocadas por Prod. de Dist",
    "0889": "Potencial Evocado Auditivo de Tronco Encef Autom",
    "0890": "Pot Evocado Aud Tronco Encef. Curta,Media e Long",
    "0891": "Audiometro de um Canal",
    "0892": "Audiometro de dois Canal",
    "0893": "Imitanciometro",
    "0894": "Imitanciometro Multifrequencial",
    "0895": "Cabine  Acustica",
    "0896": "Sistema DE Campo Livre",
    "0897": "Sistema Completo de reforço Visual (VRA)",
    "0898": "Ganho de Inserção",
    "0899": "HI-PRO",
    "0901": "Camera para Reconhecimento Facial",
    "0902": "Carrinho de Telemedicina de Videoconferencia",
    "0903": "Condensador",
    "0904": "Dermatoscopio",
    "0905": "Detector Fetal Portatil",
    "0906": "KIT Dermatoscopia",
    "0907": "KIT Medico de Diagnostico Audiologico TAB",
    "0908": "Mesa Digitalizadora",
    "0909": "Monit. Sin. Vit. Multi Port Telessaude Grau Medico",
    "0910": "Retinografo Portatil",
    "0911": "Ultrassom Portatil",
    "1001": "Aparelho de Hemodialise - Ambulatorial",
    "1002": "Aparelho de Hemodialise - Hospitalar",
    "1003": "Aparelho de Hemodialise Reserva",
    "1004": "Aparelho para Dialise Peritonial",
    # --- Transposto do dicionário atual (número de 2 dígitos → 4 dígitos) ---
    "0219": "ar condicionado",
    "0220": "camara frigorifica",
    "0224": "camara para conservacao de hemoderivados/imuno/termolabeis",
    "0225": "camara para conservacao de imunobiologicos",
    "0226": "condensador",
    "0227": "freezer cientifico",
    "0228": "grupo gerador (101 a 300 kva)",
    "0229": "grupo gerador (8 a 100 kva)",
    "0230": "grupo gerador (acima de 300 kva)",
    "0243": "grupo gerador de 1.500 kva (minimo)",
    "0266": "refrigerador",
    "0565": "grupo gerador portatil (ate 7 kva)",
    "0667": "caminhao bau refrigerado",
    "0668": "embarcacao para transporte com motor popa (ate 12 pessoas)",
    "0669": "empilhadeira",
    "0670": "veiculo utilitario (tipo furgao)",
    "0679": "veiculo pick-up cabine dupla 4x4 (diesel)",
}

# Rótulos que substituem tapa-buracos já presentes no dicionário — as chaves
# 77 a 81 foram registradas com o valor literal "Não encontrado nos dicionários
# oficiais" só para o `custom_dictionary_coverage` passar. Procedência:
#
#   77  Portaria SAES/MS nº 1.619/2024, Art. 3º §4º — texto oficial conferido
#   78, 79
#       Portaria SAES/MS nº 2.085/2024, Anexo II, via Conass Informa 156/2024
#   80  Portaria GM/MS nº 4.876/2024, operacionalizada pela SAES/MS nº
#       2.070/2024. Não foi possível ler o texto oficial (bvsms recusou
#       conexão); a sigla aparece por extenso no nome das próprias equipes
#       cadastradas ("EAP-DESINST ESTADUAL SAO LUIS"). Rótulo abreviado: o
#       nome completo tem 137 caracteres contra ~60 do resto do arquivo.
#   81  Portaria SAES/MS nº 3.200/2025, Art. 5º — texto oficial conferido
#
# Os cinco códigos entram nos dados em 2025-11 (o 81 em 2025-12), ligados no
# CNES na mesma leva apesar de portarias de anos diferentes.
REPLACE_ROWS: dict[tuple[str, str, str], str] = {
    ("equipe", "tipo_equipe", "77"): (
        "EMAP-R - Equipe Multiprofissional de Apoio para Reabilitação"
    ),
    ("equipe", "tipo_equipe", "78"): (
        "EACP - Equipe Assistencial de Cuidados Paliativos"
    ),
    ("equipe", "tipo_equipe", "79"): (
        "EMCP - Equipe Matricial de Cuidados Paliativos"
    ),
    ("equipe", "tipo_equipe", "80"): (
        "EAP-DESINST - Equipe de Avaliação e Acompanhamento de Medidas "
        "Terapêuticas (transtorno mental em conflito com a lei)"
    ),
    ("equipe", "tipo_equipe", "81"): (
        "EqAE - Equipe de Atenção Especializada"
    ),
}


def load_staging_dictionary() -> pd.DataFrame:
    """Carrega o dicionário completo do staging.

    Returns:
        DataFrame com as colunas do dicionário, tudo como string. O `fillna`
        vem antes do `astype` de propósito: na ordem inversa, um nulo vira a
        string literal "nan".
    """
    sql = f"SELECT * FROM `{STAGING_TABLE}`"
    dicionario = bd.read_sql(
        sql, billing_project_id=BILLING_PROJECT, from_file=True
    )
    return dicionario.fillna("").astype(str)


def candidate_rows() -> list[dict[str, str]]:
    """Junta as linhas declaradas uma a uma com os rótulos de 4 dígitos.

    `NEW_ROWS` traz `id_tabela` e `nome_coluna` por linha porque cobre tabelas
    e colunas diferentes. `CODIGO_EQUIPAMENTO` é sempre a mesma coluna da mesma
    tabela, então só declara chave e valor, e os dois campos entram aqui.

    Returns:
        Linhas no formato de `NEW_ROWS`, ainda sem `cobertura_temporal`.
    """
    codigo_equipamento = [
        {
            "id_tabela": "equipamento",
            "nome_coluna": "codigo_equipamento",
            "chave": chave,
            "valor": valor,
        }
        for chave, valor in CODIGO_EQUIPAMENTO.items()
    ]
    return NEW_ROWS + codigo_equipamento


def add_rows(atual: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Acrescenta as linhas novas, ignorando as que já existem.

    A chave de identidade é (id_tabela, nome_coluna, chave), para que rodar o
    script duas vezes não duplique nada.

    Args:
        atual: Dicionário como está hoje no staging.

    Returns:
        Par (linhas acrescentadas, dicionário atualizado).
    """
    existentes = set(
        zip(
            atual.id_tabela,
            atual.nome_coluna,
            atual.chave,
            strict=True,
        )
    )
    pendentes = [
        linha
        for linha in candidate_rows()
        if (linha["id_tabela"], linha["nome_coluna"], linha["chave"])
        not in existentes
    ]

    novas = pd.DataFrame(pendentes)
    if novas.empty:
        return novas, atual

    novas["cobertura_temporal"] = COBERTURA
    novas = novas[COLUMNS]
    atualizado = pd.concat([atual[COLUMNS], novas], ignore_index=True)
    return novas, atualizado


def replace_rows(atual: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reescreve o valor de linhas que já existem no dicionário.

    Idempotente: linhas cujo valor já é o esperado não entram no relatório e
    não são tocadas.

    Args:
        atual: Dicionário como está hoje.

    Returns:
        Par (relatório das linhas alteradas, dicionário atualizado).
    """
    atualizado = atual.copy()
    relatorio = []

    for (tabela, coluna, chave), novo in REPLACE_ROWS.items():
        alvo = (
            (atualizado.id_tabela == tabela)
            & (atualizado.nome_coluna == coluna)
            & (atualizado.chave == chave)
        )
        antigos = atualizado.loc[alvo, "valor"].unique()
        if not len(antigos) or (len(antigos) == 1 and antigos[0] == novo):
            continue
        relatorio.append({"chave": chave, "de": antigos[0], "para": novo})
        atualizado.loc[alvo, "valor"] = novo

    return pd.DataFrame(relatorio), atualizado


def summarize(
    novas: pd.DataFrame, alteradas: pd.DataFrame, atualizado: pd.DataFrame
) -> None:
    """Imprime o resumo da operação.

    Args:
        novas: Linhas que serão acrescentadas.
        alteradas: Linhas existentes que terão o valor reescrito.
        atualizado: Dicionário resultante.
    """
    if novas.empty:
        print("Nenhuma linha a acrescentar.")
    else:
        print(f"Linhas a acrescentar: {len(novas)}")
        print(novas.to_string(index=False))

    print()

    if alteradas.empty:
        print("Nenhum rótulo a reescrever.")
    else:
        print(f"Rótulos a reescrever: {len(alteradas)}")
        for linha in alteradas.itertuples(index=False):
            print(f"  {linha.chave}: {linha.de!r}")
            print(f"      -> {linha.para!r}")

    print(f"\nTotal depois: {len(atualizado)}")


def upload_dictionary(path: Path) -> None:
    """Envia o CSV atualizado para o staging.

    Args:
        path: Caminho do CSV a subir.
    """
    table = bd.Table(dataset_id=DATASET_ID, table_id=TABLE_ID)
    table.create(
        path=str(path),
        if_table_exists="replace",
        if_storage_data_exists="replace",
    )
    print(f"Upload concluído: staging/{DATASET_ID}/{TABLE_ID}/")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Acrescenta códigos novos do DATASUS ao dicionário."
    )
    modo = parser.add_mutually_exclusive_group()
    modo.add_argument(
        "--apply",
        action="store_true",
        help="Grava o CSV e sobe para o staging (padrão: só dry-run).",
    )
    modo.add_argument(
        "--dry-run",
        action="store_true",
        help="Explícito; é o comportamento padrão.",
    )
    args = parser.parse_args()

    print(f"Lendo {STAGING_TABLE} ...")
    atual = load_staging_dictionary()
    novas, atualizado = add_rows(atual)
    alteradas, atualizado = replace_rows(atualizado)
    summarize(novas, alteradas, atualizado)

    if novas.empty and alteradas.empty:
        print("\nNada a fazer (dicionário já atualizado).")
        return

    if not args.apply:
        print(
            "\nDry-run. Para aplicar: "
            "uv run python update_dicionario.py --apply"
        )
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    atualizado.to_csv(OUTPUT_CSV, index=False)
    print(f"\nCSV salvo em {OUTPUT_CSV}")
    upload_dictionary(OUTPUT_CSV)
    print(
        "\nPróximo passo: cd ../../.. && "
        "uv run dbt run --select br_ms_cnes__dicionario"
    )


if __name__ == "__main__":
    main()
