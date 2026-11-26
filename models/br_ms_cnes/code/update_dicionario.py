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
    uv run update_dicionario.py --dry-run

    # Aplicar: grava o CSV e sobe para o staging dev
    uv run update_dicionario.py --apply
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
#   83      Portaria SAES/MS nº 4.288, de 22/06/2026 (DOU de 23/07/2026),
#           Anexo II — "Tabela de Equipes do CNES". Lido na reprodução do
#           Conass Informa 140/2026; o link do DOU é
#           in.gov.br/web/dou/-/portaria-saes/ms-n-4.288-de-22-de-junho-de-2026-720815464
#           Aparece nos dados em 2026-09 (uma equipe em 3505500, "CUIDADOS
#           PALIA PEDIATRIA"), e derrubava o `custom_dictionary_coverage`.
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
    {
        "id_tabela": "equipe",
        "nome_coluna": "tipo_equipe",
        "chave": "83",
        "valor": (
            "EMCP-Ped - Equipe Macrorregional de Cuidados Paliativos com "
            "Pediatra"
        ),
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

# Rótulos com a grafia da Tabela Unificada do SIGTAP, competência 2026-10
# (TabelaUnificada_202610_v2610271043.zip, em
# ftp2.datasus.gov.br/pub/sistemas/tup/downloads/). Tabela de origem por
# coluna:
#
#   tipo_habilitacao                  tb_habilitacao (CO_HABILITACAO, sem o
#                                     zero à esquerda)
#   tipo_servico_especializado e
#   tipo_servico_especializado_unico  tb_servico (CO_SERVICO)
#   subtipo_servico_especializado     tb_servico_classificacao (CO_SERVICO
#                                     seguido de CO_CLASSIFICACAO)
SIGTAP_LABELS: dict[tuple[str, str], dict[str, str]] = {
    ("habilitacao", "tipo_habilitacao"): {
        "406": "Serviço de Especialidade em Saúde Bucal",
        "638": "CENTRO DE CONVIVÊNCIA BÁSICO",
        "639": "CENTRO DE CONVIVÊNCIA INTERMEDIÁRIO",
        "640": "CENTRO DE CONVIVÊNCIA AMPLIADO",
        "815": "Implante Percutâneo de Válvula Aórtica (TAVI)",
        "1420": (
            "Hospital de Referência à Gestação e ao Puerpério de Alto Risco "
            "-HGPAR"
        ),
        "1515": "Incremento financeiro à Terapia Renal Substitutiva - TRS",
        "1618": "Neurocirurgia Endovascular/Trombectomia Mecanica",
        "1722": "Tratamentos Integrados Sincrônicos em Oncologia",
        "2308": (
            "Serviço de Referência em Tratamento da Pessoa com Falência "
            "Intestinal"
        ),
        "2436": "Incremento Financeiro SNT TMO Nível A",
        "2437": "Incremento Financeiro SNT TMO Nível B",
        "2438": "Incremento Financeiro SNT TMO Nível C",
        "2439": "Incremento Financeiro SNT TMO Nível D",
        "2440": "Incremento Financeiro SNT TMO Nível E",
        "2441": "Incremento Financeiro SNT Pâncreas Nível A",
        "2442": "Incremento Financeiro SNT Pâncreas Nível B",
        "2443": "Incremento Financeiro SNT Pâncreas Nível C",
        "2446": "Incremento Financeiro SNT Rim Nível A",
        "2447": "Incremento Financeiro SNT Rim Nível B",
        "2448": "Incremento Financeiro SNT Rim Nível C",
        "2449": "Incremento Financeiro SNT Rim Nível D",
        "2450": "Incremento Financeiro SNT Rim Nível E",
        "2451": "Incremento Financeiro SNT Fígado Nível A",
        "2452": "Incremento Financeiro SNT Fígado Nível B",
        "2453": "Incremento Financeiro SNT Fígado Nível C",
        "2454": "Incremento Financeiro SNT Fígado Nível D",
        "2455": "Incremento Financeiro SNT Fígado Nível E",
        "2456": "Incremento Financeiro SNT Pulmão Nível A",
        "2458": "Incremento Financeiro SNT Pulmão Nível C",
        "2461": "Incremento Financeiro SNT Coração Nível A",
        "2462": "Incremento Financeiro SNT Coração Nível B",
        "2463": "Incremento Financeiro SNT Coração Nível C",
        "2464": "Incremento Financeiro SNT Coração Nível D",
        "2465": "Incremento Financeiro SNT Coração Nível E",
        "2466": "Transplante de intestino delgado ou multivisceral",
        "2467": "Transplante de Membrana Amniótica",
        "2468": "Banco de Multitecidos",
        "2469": "Transplante Pediátrico Tipo I",
        "2470": "Transplante Pediátrico Tipo II",
        "2471": (
            "Transplante de Células Progenitoras Hematopoéticas Alogênico "
            "Aparentado com HLA haploidêntico"
        ),
        "2472": (
            "Transplante de Células Progenitoras Hematopoéticas Autólogo "
            "Pediátrico I"
        ),
        "2473": (
            "Transplante de Células Progenitoras Hematopoéticas Autólogo "
            "Pediátrico II"
        ),
        "2474": (
            "Transplante de Células Progenitoras Hematopoéticas Alogênico "
            "Aparentado c/ HLA Idêntico Pediátrico I"
        ),
        "2475": (
            "Transplante de Células Progenitoras Hematopoéticas Alogênico "
            "Aparentado c/ HLA Idêntico Ped II"
        ),
        "2476": (
            "Transplante de Células Progenitoras Hematopoéticas Alogênico "
            "Aparentado c/ HLA Haploidêntico - Ped I"
        ),
        "2477": (
            "Transplante de Células Progenitoras Hematopoéticas Alogênico "
            "Aparentado c/ HLA Haploidêntico Ped II"
        ),
        "2478": (
            "Transplante de Células Progenitoras Hematopoéticas Não "
            "Aparentado Pediátrico I"
        ),
        "2479": (
            "Transplante de Células Progenitoras Hematopoéticas Não "
            "Aparentado Pediátrico II"
        ),
        "2482": "Transplante de Rim - Pediátrico Tipo I",
        "2483": "Transplante de Rim - Pediátrico Tipo II",
        "2484": "Transplante de Fígado - Pediátrico Tipo I",
        "2485": "Transplante de Fígado - Pediátrico Tipo II",
        "2486": "Transplante de Pulmão - Pediátrico Tipo I",
        "2487": "Transplante de Pulmão - Pediátrico Tipo II",
        "2488": "Transplante de Coração - Pediátrico Tipo I",
        "2489": "Transplante de Coração - Pediátrico Tipo II",
        "2490": (
            "Transplante de Intestino Delgado ou Multivisceral - Pediátrico "
            "Tipo I"
        ),
        "2902": "Programa Mais Acesso a Especialistas - Componente Cirurgias",
        "3516": "Terapia Génica",
        "3801": "Programa Mais Acesso a Especialistas",
        "3803": "Agora Tem Especialistas Modalidade 2 Equipes Volantes",
        "3804": "Agora Tem Especialistas Modalidade 3 Unidades Móveis",
        "3805": "Agora Tem Especialistas - Componente Créditos Financeiro",
        "3806": "Agora Tem Especialistas - Componente Ressarcimento ao SUS",
        "3807": (
            "Componente Acesso à Radioterapia do Programa Agora Tem "
            "Especialistas"
        ),
        "3808": "Agora Tem Especialistas - Teleatendimento em Saúde Mental",
        "3811": "Agora tem Especialistas 3 Unidade Móvel Exame de Imagem",
        "3812": "Agora tem Especialistas 3 Unidade Móvel Oftalmologia",
        "3813": "Agora tem Especialistas 3 Unidade Móvel Saúde da Mulher",
        "3814": "Agora tem Especialistas 3 Unidade Móvel Cardiologia",
        "3815": "Agora tem Especialistas 3 Unidade Móvel Saúde Bucal",
    },
    ("servico_especializado", "tipo_servico_especializado"): {
        "171": "Cuidados Paliativos",
        "172": "Atenção ao Luto Materno e Parental",
        "175": "Cirúrgia Robótica",
    },
    ("servico_especializado", "subtipo_servico_especializado"): {
        "112007": (
            "Ambulatório de Seguimento do RN e Criança Egresso de Unidade "
            "Neonatal"
        ),
        "112008": "Ambulatório  de Gestação e Puerpério de Alto Risco",
        "114008": "Odontopediatria",
        "114009": "Disfunsão temporomandibular",
        "114010": "Estomatologia",
        "114011": "Implantodondia",
        "114012": "Odontogeriatria",
        "114013": "Ortodontia",
        "114014": "Prótese Dentária",
        "114015": "Radiologia Odontológica e Imaginologia",
        "114016": "Acupuntura na Odontologia",
        "114017": "Ortopedia funcional dos maxilares",
        "114018": "Odontologia hospitalar",
        "114019": "Patologia Oral e Máxilo Fácial",
        "114020": "Prótese Buco Maxilo Facial",
        "115010": (
            "DESINST de Pessoas com Transtorno  Mental em Conflito com a Lei"
        ),
        "115011": "Promoção da Saúde Mental, Protagonismo e Cidadania",
        "125010": "Manipulação, Reconstituição e Diluição de Medicamentos",
        "141005": "Vigilância de Zoonoses",
        "154003": "Banco de Multitecidos",
        "160004": "TELEDIAGNOSTICO",
        "160005": "TELE-EDUCACAO",
        "160006": "TELECONSULTA",
        "160007": "TELEMONITORAMENTO",
        "160008": "TELEORIENTACAO",
        "160009": "TELEINTERCONSULTA",
        "160010": "TELETRIAGEM",
        "160011": "TELERREGULACAO",
        "164012": "Confecção de OPM Auxiliares de Locomoção e Ortopédicas",
        "168003": "Terapia Génica",
        "171001": "Abordagem Paliativa",
        "171002": "Apoio Matricial em Cuidados Paliativos",
        "171003": "Cuidados Paliativos Especializados",
        "172001": "Assistência Multiprofissional ao luto materno e parental",
        "175001": "Prostatectomia",
    },
    ("servico_especializado", "tipo_servico_especializado_unico"): {
        "171": "Cuidados Paliativos",
        "172": "Atenção ao Luto Materno e Parental",
        "175": "Cirúrgia Robótica",
    },
}

NO_MEANING_LABEL = (
    "Código {}: sem significado nas fontes oficiais (nota da BD)"
)

CODES_WITHOUT_MEANING: dict[tuple[str, str], list[str]] = {
    ("servico_especializado", "subtipo_servico_especializado"): ["170002"],
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
#
# Os 38 incentivos abaixo entraram no staging de dev com o rótulo de
# `NO_MEANING_LABEL`, antes de as portarias serem achadas. Os rótulos têm a
# grafia do ato que criou cada código. Procedência:
#
#   8116, 8117           Portaria GM/MS nº 1.992, de 24/11/2023, Art. 1º
#   8119                 Portaria SAES/MS nº 602, de 28/07/2023, Art. 1º
#   8121 a 8124          Portaria SAES/MS nº 4.020, de 10/04/2026, Anexo II
#   8125 a 8127          Portaria SAES/MS nº 4.288, de 22/06/2026, Anexo III
#   8256 a 8260, 8263,
#   8264, 8266 a 8270    Portaria SAS/MS nº 1.535, de 25/09/2017, Anexo
#   8271 a 8280          Portaria SAES/MS nº 474, de 22/04/2021, Anexo I
#   8285                 Portaria SAES/MS nº 216, de 01/07/2022, Anexo II;
#                        excluído pela Portaria SAES/MS nº 2.902, de
#                        26/06/2025, Art. 5º §2º
#   8286 a 8288, 8290,
#   8291                 Portaria SAES/MS nº 1.148, de 21/12/2023, Anexo IV
#
# Os números 81.16 a 81.39 já foram incentivos de equipes de saúde prisional
# (Portaria SAS/MS nº 305/2014, Anexo II), desativados pelo Art. 10 da
# Portaria SAES/MS nº 37, de 18/01/2021. Os rótulos acima são os dos números
# reaproveitados a partir de 2023.
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
    ("estabelecimento", "tipo_unidade", "16"): NO_MEANING_LABEL.format("16"),
    ("incentivos", "tipo_habilitacao", "8116"): "DRC Categoria I",
    ("incentivos", "tipo_habilitacao", "8117"): "DRC Categoria II",
    ("incentivos", "tipo_habilitacao", "8119"): (
        "Hospital Sem Fins Lucrativos 100% SUS"
    ),
    ("incentivos", "tipo_habilitacao", "8121"): (
        "Qualificação em Doação e Transplantes - IFQ-DOT I"
    ),
    ("incentivos", "tipo_habilitacao", "8122"): (
        "Qualificação em Doação e Transplantes - IFQ-DOT II"
    ),
    ("incentivos", "tipo_habilitacao", "8123"): (
        "Qualificação em Doação e Transplantes - IFQ-DOT III"
    ),
    ("incentivos", "tipo_habilitacao", "8124"): (
        "Qualificação em Doação e Transplantes - IFQ-DOT IV"
    ),
    ("incentivos", "tipo_habilitacao", "8125"): (
        "Equipes de Apoio Assistencial em Cuidados Paliativos"
    ),
    ("incentivos", "tipo_habilitacao", "8126"): (
        "Equipes Macrorregionais de Cuidados Paliativos"
    ),
    ("incentivos", "tipo_habilitacao", "8127"): (
        "Equipes Macrorregionais de Cuidados Paliativos com Pediatra"
    ),
    ("incentivos", "tipo_habilitacao", "8256"): (
        "UPA 24h Nova - Qualificada Opção I"
    ),
    ("incentivos", "tipo_habilitacao", "8257"): (
        "UPA 24h Nova - Qualificada Opção II"
    ),
    ("incentivos", "tipo_habilitacao", "8258"): (
        "UPA 24h Nova - Qualificada Opção IV"
    ),
    ("incentivos", "tipo_habilitacao", "8259"): (
        "UPA 24h Nova - Qualificada Opção VI"
    ),
    ("incentivos", "tipo_habilitacao", "8260"): (
        "UPA 24h Nova - Qualificada Opção VII"
    ),
    ("incentivos", "tipo_habilitacao", "8263"): (
        "UPA 24h Ampliada - Habilitada/Qualificada Opção IV"
    ),
    ("incentivos", "tipo_habilitacao", "8264"): (
        "UPA 24h Ampliada - Habilitada/Qualificada Opção VI"
    ),
    ("incentivos", "tipo_habilitacao", "8266"): (
        "UPA 24h Nova - Habilitada Opção I"
    ),
    ("incentivos", "tipo_habilitacao", "8267"): (
        "UPA 24h Nova - Habilitada Opção II"
    ),
    ("incentivos", "tipo_habilitacao", "8268"): (
        "UPA 24h Nova - Habilitada Opção IV"
    ),
    ("incentivos", "tipo_habilitacao", "8269"): (
        "UPA 24h Nova - Habilitada Opção VI"
    ),
    ("incentivos", "tipo_habilitacao", "8270"): (
        "UPA 24h Nova - Habilitada Opção VII"
    ),
    ("incentivos", "tipo_habilitacao", "8271"): (
        "Enfermaria Clínica de Retaguarda - Novos"
    ),
    ("incentivos", "tipo_habilitacao", "8272"): (
        "Enfermaria Clínica de Retaguarda - Qualificados"
    ),
    ("incentivos", "tipo_habilitacao", "8273"): (
        "UTI Adulto RUE Tipo II - Novos"
    ),
    ("incentivos", "tipo_habilitacao", "8274"): (
        "UTI Adulto RUE Tipo II - Qualificados"
    ),
    ("incentivos", "tipo_habilitacao", "8275"): (
        "UTI Pediátrico RUE Tipo II - Novos"
    ),
    ("incentivos", "tipo_habilitacao", "8276"): (
        "UTI Pediátrico RUE Tipo II - Qualificados"
    ),
    ("incentivos", "tipo_habilitacao", "8277"): (
        "UTI Adulto RUE Tipo III - Novos"
    ),
    ("incentivos", "tipo_habilitacao", "8278"): (
        "UTI Adulto RUE Tipo III - Qualificados"
    ),
    ("incentivos", "tipo_habilitacao", "8279"): (
        "UTI Pediátrico RUE Tipo III - Novos"
    ),
    ("incentivos", "tipo_habilitacao", "8280"): (
        "UTI Pediátrico RUE Tipo III - Qualificados"
    ),
    ("incentivos", "tipo_habilitacao", "8285"): (
        "Atenção especializada ao seguimento do recém-nascido e crianças "
        "egressos de unidades neonatais (ANEO)"
    ),
    ("incentivos", "tipo_habilitacao", "8286"): (
        "Transporte Sanitário Adaptado I"
    ),
    ("incentivos", "tipo_habilitacao", "8287"): (
        "Transporte Sanitário Adaptado II"
    ),
    ("incentivos", "tipo_habilitacao", "8288"): (
        "Transporte Sanitário Adaptado III"
    ),
    ("incentivos", "tipo_habilitacao", "8290"): (
        "Núcleo de Atenção à Criança e Adolescente com Transtorno do "
        "Espectro Autista"
    ),
    ("incentivos", "tipo_habilitacao", "8291"): (
        "Incentivo de 20% no custeio do CER habilitado com a modalidade de "
        "reabilitação intelectual"
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
    """Junta as linhas declaradas uma a uma com as declaradas por coluna.

    `NEW_ROWS` traz `id_tabela` e `nome_coluna` por linha porque cobre tabelas
    e colunas diferentes. `CODIGO_EQUIPAMENTO`, `SIGTAP_LABELS` e
    `CODES_WITHOUT_MEANING` agrupam as chaves por coluna, e os dois campos
    entram aqui. Os códigos de `CODES_WITHOUT_MEANING` recebem o rótulo de
    `NO_MEANING_LABEL`.

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
    sigtap = [
        {
            "id_tabela": table_and_column[0],
            "nome_coluna": table_and_column[1],
            "chave": chave,
            "valor": valor,
        }
        for table_and_column, labels in SIGTAP_LABELS.items()
        for chave, valor in labels.items()
    ]
    without_meaning = [
        {
            "id_tabela": table_and_column[0],
            "nome_coluna": table_and_column[1],
            "chave": chave,
            "valor": NO_MEANING_LABEL.format(chave),
        }
        for table_and_column, chaves in CODES_WITHOUT_MEANING.items()
        for chave in chaves
    ]
    return NEW_ROWS + codigo_equipamento + sigtap + without_meaning


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
        print("\nDry-run. Para aplicar: uv run update_dicionario.py --apply")
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
