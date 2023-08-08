# -*- coding: utf-8 -*-
"""
General purpose functions for the br_mp_pep_cargos_funcoes project
"""

import os
import time
from pipelines.datasets.br_mp_pep_cargos_funcoes.constants import constants
from pipelines.utils.utils import log


def wait_file_download(year: int, timeout=60 * 6):
    start_time = time.time()
    end_time = start_time + timeout

    file_exists = False

    while not file_exists:
        time.sleep(1.0)
        files = [
            f
            for f in os.listdir(constants.TMP_DATA_DIR.value)
            if not f.endswith(".crdownload")
        ]
        if len(files) > 0:
            log(f"Time to download {year}, {time.time() - start_time} seconds")
            break
        if time.time() > end_time:
            raise Exception(f"Timeout to download xlsx for {year}")
        continue

    return True


def move_from_tmp_dir(year: int):
    files = os.listdir(constants.TMP_DATA_DIR.value)
    log(f"Files in tmp dir: {files}")
    assert len(files) == 1
    src = os.path.join(constants.TMP_DATA_DIR.value, files[0])
    dest_file_name = f"{str(year)}.xlsx"
    dest = os.path.join(constants.INPUT_DIR.value, dest_file_name)
    os.rename(src, dest)
    log(f"Files after moved: {os.listdir(constants.TMP_DATA_DIR.value)}")
    assert os.path.exists(dest)


def get_normalized_values_by_col():
    return {
        "mes": {
            "Jan": 1,
            "Fev": 2,
            "Mar": 3,
            "Abr": 4,
            "Mai": 5,
            "Jun": 6,
            "Jul": 7,
            "Ago": 8,
            "Set": 9,
            "Out": 10,
            "Nov": 11,
            "Dez": 12,
        },
        "regiao": {
            "CO": "Centro-Oeste",
            "N": "Norte",
            "NE": "Nordeste",
            "S": "Sul",
            "SE": "Sudeste",
        },
        "orgao_superior": {
            "Presidencia Da Republica": "Presidência da República",
            "Defensoria Publica Da Uniao": "Defensoria Pública da União",
            "Minist. Da Justica E Seguranca Publica": "Ministério da Justiça e Segurança Pública",
            "Minist.Da Agricultura,Pecuaria E Abast.": "Ministério da Agricultura, Pecuária e Abastecimento",
            "Ministerio Ciencia Tec.Inov.Comunicacoes": "Ministério da Ciência, Tecnologia, Inovações e Comunicações",
            "Ministerio Da Cultura": "Ministério da Cultura",
            "Ministerio Da Economia": "Ministério da Economia",
            "Ministerio Da Educacao": "Ministério da Educação",
            "Ministerio Da Infraestrutura": "Ministério da Infraestrutura",
            "Ministerio Da Previdencia Social": "Ministério da Previdência Social",
            "Ministerio Da Saude": "Ministério da Saúde",
            "Ministerio Das Comunicacoes": "Ministério das Comunicações",
            "Ministerio Das Relacoes Exteriores": "Ministério das Relações Exteriores",
            "Ministerio De Minas E Energia": "Ministério de Minas e Energia",
            "Ministerio Do Desenvolvimento Regional": "Ministério do Desenvolvimento Regional",
            "Ministerio Do Esporte": "Ministério do Esporte",
            "Ministerio Do Meio Ambiente": "Ministério do Meio Ambiente",
            "Ministerio Do Planej. Desenv. E Gestao": "Ministério do Planejamento, Desenvolvimento e Gestão",
            "Ministerio Do Trabalho E Emprego": "Ministério do Trabalho e Emprego",
            "Ministerio Ind. Com. Exterior E Serviços": "Ministério da Indústria, Comércio Exterior e Serviços",
            "Ministerio Do Turismo": "Ministério do Turismo",
            "-": None,
            "Ministerio Do Desenvolvimento Agrario": "Ministério do Desenvolvimento Agrário",
            "Ministerio Da Cidadania": "Ministério da Cidadania",
            "Ministerio Das Cidades": "Ministério das Cidades",
            "Controladoria-Geral Da Uniao": "Controladoria-Geral da União",
            "Min. Da Mulher, Familia E Dir. Humanos": "Ministério da Mulher, Família e Direitos Humanos",
            "Ministerio Do Trabalho E Previdencia": "Ministério do Trabalho e Previdência",
        },
        "orgao": {
            "Advocacia-Geral Da Uniao": "Advocacia-Geral da União",
            "Comando Da Aeronautica": "Comando da Aeronáutica",
            "Comando Da Marinha": "Comando da Marinha",
            "Comando Do Exercito": "Comando do Exército",
            "Defensoria Publica Da Uniao": "Defensoria Pública da União",
            "Departamento De Policia Federal": "Departamento de Polícia Federal",
            "Minist. Da Justica E Seguranca Publica": "Ministério da Justiça e Segurança Pública",
            "Minist.Da Agricultura,Pecuaria E Abast.": "Ministério da Agricultura, Pecuária e Abastecimento",
            "Ministerio Ciencia Tec.Inov.Comunicacoes": "Ministério da Ciência, Tecnologia, Inovações e Comunicações",
            "Ministerio Da Cultura": "Ministério da Cultura",
            "Ministerio Da Defesa": "Ministério da Defesa",
            "Ministerio Da Economia": "Ministério da Economia",
            "Ministerio Da Educacao": "Ministério da Educação",
            "Ministerio Da Infraestrutura": "Ministério da Infraestrutura",
            "Ministerio Da Previdencia Social": "Ministério da Previdência Social",
            "Ministerio Da Saude": "Ministério da Saúde",
            "Ministerio Das Comunicacoes": "Ministério das Comunicações",
            "Ministerio Das Relacoes Exteriores": "Ministério das Relações Exteriores",
            "Ministerio De Minas E Energia": "Ministério de Minas e Energia",
            "Ministerio Do Desenvolvimento Regional": "Ministério do Desenvolvimento Regional",
            "Ministerio Do Esporte": "Ministério do Esporte",
            "Ministerio Do Meio Ambiente": "Ministério do Meio Ambiente",
            "Ministerio Do Planej. Desenv. E Gestao": "Ministério do Planejamento, Desenvolvimento e Gestão",
            "Ministerio Do Trabalho E Emprego": "Ministério do Trabalho e Emprego",
            "Ministerio Ind. Com. Exterior E Serviços": "Ministério da Indústria, Comércio Exterior e Serviços",
            "Presidencia Da Republica": "Presidência da República",
            "Vice-Presidencia Da Republica": "Vice-Presidência da República",
            "Agencia Espacial Brasileira": "Agência Espacial Brasileira",
            "Agencia Nac Petroleo Gas Nat Biocombusti": "Agência Nacional do Petróleo, Gás Natural e Biocombustíveis",
            "Agencia Nacional De Energia Eletrica": "Agência Nacional de Energia Elétrica",
            "Agencia Nacional De Telecomunicacoes": "Agência Nacional de Telecomunicações",
            "Agencia Nacional De Vigilancia Sanitaria": "Agência Nacional de Vigilância Sanitária",
            "Caixa De Financiamento Imob.Aeronautica": "Caixa de Financiamento Imobiliário da Aeronáutica",
            "Comissao De Valores Mobiliarios": "Comissão de Valores Mobiliários",
            "Comissao Nacional De Energia Nuclear": "Comissão Nacional de Energia Nuclear",
            "Conselho Administ.De Defesa Economica": "Conselho Administrativo de Defesa Econômica",
            "Departamento Nac. De Producao Mineral": "Departamento Nacional de Produção Mineral",
            "Depto. Nacional De Obras Contra As Secas": "Departamento Nacional de Obras Contra as Secas",
            "Fundo Nacional De Desenvolv. Da Educacao": "Fundo Nacional de Desenvolvimento da Educação",
            "Inst. Br. Meio Amb. Rec. Nat. Renovaveis": "Instituto Brasileiro do Meio Ambiente e dos Recursos Naturais Renováveis",
            "Inst.Nac.Metrologia,Norm.E Qual.Indl.": "Instituto Nacional de Metrologia, Qualidade e Tecnologia",
            "Inst.Nacional De Est.E Pesq.Educacionais": "Instituto Nacional de Estudos e Pesquisas Educacionais",
            "Instituto Brasileiro De Turismo": "Instituto Brasileiro de Turismo",
            "Instituto Do Patr.Hist.E Art. Nacional": "Instituto do Patrimônio Histórico e Artístico Nacional",
            "Instituto Nac. Da Propriedade Industrial": "Instituto Nacional da Propriedade Industrial",
            "Instituto Nac. De Coloniz E Ref Agraria": "Instituto Nacional de Colonização e Reforma Agrária",
            "Instituto Nacional De Seguro Social": "Instituto Nacional do Seguro Social",
            "Superintendencia De Seguros Privados": "Superintendência de Seguros Privados",
            "Superintendencia Zona Franca De Manaus": "Superintendência da Zona Franca de Manaus",
            "-": None,
            "Conselho Nac.De Desen.Cien.E Tecnologico": "Conselho Nacional de Desenvolvimento Científico e Tecnológico",
            "Fund Coord Aperf Pessoal Nivel Superior": "Fundação Coordenação de Aperfeiçoamento de Pessoal de Nível Superior",
            "Fund. Inst. Brasil. Geog. E Estatistica": "Fundação Instituto Brasileiro de Geografia e Estatística",
            "Fund.Jorge Duprat Fig. Seg. Med.Trabalho": "Fundação Jorge Duprat Figueiredo de Segurança e Medicina do Trabalho",
            "Fundacao Alexandre De Gusmao": "Fundação Alexandre de Gusmão",
            "Fundacao Biblioteca Nacional": "Fundação Biblioteca Nacional",
            "Fundacao Casa De Rui Barbosa": "Fundação Casa de Rui Barbosa",
            "Fundacao Cultural Palmares": "Fundação Cultural Palmares",
            "Fundacao Escola Nacional De Adm. Publica": "Fundação Escola Nacional de Administração Pública",
            "Fundacao Joaquim Nabuco": "Fundação Joaquim Nabuco",
            "Fundacao Nacional De Artes": "Fundação Nacional de Artes",
            "Fundacao Nacional De Saude": "Fundação Nacional de Saúde",
            "Fundacao Nacional Do Indio": "Fundação Nacional do Índio",
            "Fundacao Osorio": "Fundação Osório",
            "Fundacao Oswaldo Cruz": "Fundação Oswaldo Cruz",
            "Instituto De Pesquisa Economica Aplicada": "Instituto de Pesquisa Econômica Aplicada",
            "Ministerio Do Desenvolvimento Agrario": "Ministério do Desenvolvimento Agrário",
            "Min Do Desenv Agr E Agric Familiar": "Ministério do Desenvolvimento Agrário",
            "Agencia Brasileira De Inteligencia": "Agência Brasileira de Inteligência",
            "Departamento Nac.De Infraest. De Transp.": "Departamento Nacional de Infraestrutura de Transportes",
            "Ministerio Da Cidadania": "Ministério da Cidadania",
            "Ministerio Das Cidades": "Ministério das Cidades",
            "Ministerio Do Turismo": "Ministério do Turismo",
            "Instituto De Pesq. Jardim Botanico Do Rj": "Instituto de Pesquisa Jardim Botânico do Rio de Janeiro",
            "Depto. De Policia Rodoviaria Federal": "Departamento de Polícia Rodoviária Federal",
            "Instituto Chico Mendes Conserv.Biodiver.": "Instituto Chico Mendes de Conservação da Biodiversidade",
            "Superintendencia Do Desenv. Da Amazonia": "Superintendência do Desenvolvimento da Amazônia",
            "Superintendencia Do Desenv. Do Nordeste": "Superintendência do Desenvolvimento do Nordeste",
            "Instituto Brasileiro De Museus": "Instituto Brasileiro de Museus",
            "Superint.Nac.De Previdencia Complementar": "Superintendência Nacional de Previdência Complementar",
            "Sup.De Desenvolvimento Do Centro Oeste": "Superintendência de Desenvolvimento do Centro-Oeste",
            "Controladoria-Geral Da Uniao": "Controladoria-Geral da União",
            "Min. Da Mulher, Familia E Dir. Humanos": "Ministério da Mulher, Família e Direitos Humanos",
            "Cons. De Controle De Ativ. Financeiras": "Conselho de Controle de Atividades Financeiras",
            "Ministerio Do Trabalho E Previdencia": "Ministério do Trabalho e Previdência",
            "Min Da Integ E Do Desenv Regional": "Ministério da Integração e do Desenvolvimento Regional",
            "Ministerio Dos Povos Indigenas": "Ministério dos Povos Indígenas",
        },
        "sexo": {"Fem": "Feminino", "Mas": "Masculino"},
        "raca_cor": {
            "Indigena": "Indígena",
            "Nao Informado": "Não informado",
        },
        "faixa_etaria": {"Ate 30 anos": "Até 30 anos"},
        "escolaridade_servidor": {
            "Ensino Medio": "Ensino Médio",
            "Ensino Superior": "Ensino Superior",
            "Doutorado": "Doutorado",
            "Especializacao": "Especialização",
            "4A. Serie Do Primeiro Grau Completa": "4ª Série do Primeiro Grau Completa",
            "Mestrado": "Mestrado",
            "Ensino Fundamental": "Ensino Fundamental",
            "Aperfeicoamento": "Aperfeiçoamento",
            "S/Info": "Sem informação",
            "Superior Incompleto": "Superior Incompleto",
            "Analfabeto": "Analfabeto",
            "Pos Doutorado": "Pós-Doutorado",
            "Aperfeicoamento (Lei 12772/12 Art 18)(T)": "Aperfeiçoamento (Lei 12772/12 Art 18)(T)",
            "Livre Docencia": "Livre Docência",
            "Ensino Fundamental Incompleto": "Ensino Fundamental Incompleto",
            "Especializacao Nivel Superior(T)": "Especialização Nível Superior(T)",
            "Graduacao (Nivel Superior Completo)(T)": "Graduação (Nível Superior Completo)(T)",
            "Aperfeicoamento Nivel Superior(T)": "Aperfeiçoamento Nível Superior(T)",
            "Mestrado(T)": "Mestrado(T)",
            "Doutorado(T)": "Doutorado(T)",
            "Especializacao Nivel Medio(T)": "Especialização Nível Médio(T)",
            "Especializacao - Na(T)": "Especialização - Na(T)",
            "Licenciatura(T)": "Licenciatura(T)",
            "Tecnico (Nivel Medio Completo)(T)": "Técnico (Nível Médio Completo)(T)",
            "Pos-Doutorado(T)": "Pós-Doutorado(T)",
            "Segundo Grau Incompleto": "Segundo Grau Incompleto",
            "Aperfeicoamento Nivel Medio(T)": "Aperfeiçoamento Nível Médio(T)",
            "Pos-Graduacao(T)": "Pós-Graduação(T)",
            "Curso Capacit/Qualific Profissi Min 180H(T)": "Curso Capacitação/Qualificação Profissional Min 180H(T)",
            "Curso Qualificacao Profissional Min 360H(T)": "Curso Qualificação Profissional Min 360H(T)",
            "Curso Qualificacao Profissional Min 180H(T)": "Curso Qualificação Profissional Min 180H(T)",
            "Primeiro Grau Incomp.-Ate A 4A.Serie Incomp.": "Primeiro Grau Incompleto - Até a 4ª Série Incompleta",
            "Nivel Médio(T)": "Nível Médio(T)",
            "Curso Qualificacao Profissional Min 250H(T)": "Curso Qualificação Profissional Min 250H(T)",
            "Bacharel(T)": "Bacharel(T)",
        },
    }
