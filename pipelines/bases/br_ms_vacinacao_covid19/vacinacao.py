import os
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

def url_scrapper(url: str, pattern: str) -> list:
    """
    Get list of dictionaries where the values are the urls and the keys are the url's identifiers.
    url: source where the urls should be grabbed
    pattern: identifier's regex pattern
    """
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    all_links = soup.find_all("a")
    links = []
    for i in range(len(all_links)):
        text = all_links[i].text.strip()
        if bool(re.match(pattern, text)):
            link = all_links[i].attrs["href"]
            links.append({text:link})

    return links

def download_raw(group, identifier, partition_info):
    partitions = url_scrapper(url=partition_info[group]["url"], pattern=partition_info[group]["pattern"])
    try:
        url = {list(partition.keys())[0]:list(partition.values())[0] for partition in partitions if list(partition.keys())[0]==identifier}[identifier]
        filename = identifier.replace(' ','_').lower()
        path = "/tmp/data/br_ms_vacinacao_covid19/input/" + filename + ".csv"
        print(f"Downloading raw files for {identifier}.")
        os.system("wget -O {0} {1}".format(path, url)) #user agent is block by the ms website, so we use linux wget here instead of python's wget
    except:
        print(f'Cannot download {identifier}')

def build_microdados(uf, df, munic, n_chunk):
  path = "output/microdados/sigla_uf={}".format(uf)
  os.system(f'mkdir -p {path}')
  df = df[['document_id',
           'paciente_id',
           'paciente_idade',
           'paciente_dataNascimento',
           'paciente_enumSexoBiologico',
           'paciente_racaCor_codigo',
           'paciente_endereco_coIbgeMunicipio',
           'paciente_endereco_coPais',
           'paciente_endereco_cep',
           'paciente_nacionalidade_enumNacionalidade',
           'estabelecimento_valor',
           'estabelecimento_razaoSocial',
           'estalecimento_noFantasia',
           'estabelecimento_municipio_codigo',
           'vacina_grupoAtendimento_codigo',
           'vacina_categoria_codigo',
           'vacina_lote',
           'vacina_fabricante_nome',
           'vacina_fabricante_referencia',
           'vacina_dataAplicacao',
           'vacina_descricao_dose',
           'vacina_codigo',
           'sistema_origem']]

  df.columns = ['id_documento',
                'id_paciente',
                'idade_paciente',
                'data_nascimento_paciente',
                'sexo_paciente',
                'raca_cor_paciente',
                'id_municipio_6_endereco_paciente',
                'pais_endereco_paciente',
                'cep_endereco_paciente',
                'nacionalidade_paciente',
                'id_estabelecimento',
                'razao_social_estabelecimento',
                'nome_fantasia_estabelecimento',
                'id_municipio_6_estabelecimento',
                'grupo_atendimento_vacina',
                'categoria_vacina',
                'lote_vacina',
                'nome_fabricante_vacina',
                'referencia_fabricante_vacina',
                'data_aplicacao_vacina',
                'dose_vacina',
                'codigo_vacina',
                'sistema_origem']
  
  #-----------------#
  # paciente
  #-----------------#

  #print(df[df['raca_cor'].isnull()]) # https://stackoverflow.com/questions/47333227/pandas-valueerror-cannot-convert-float-nan-to-integer
  df = df.dropna(subset=['raca_cor_paciente']) # dropping the few observations with null information

  df['raca_cor_paciente'] = df['raca_cor_paciente'].astype('int64')
  df['raca_cor_paciente'] = df['raca_cor_paciente'].astype('string')

  df = df.merge(munic[['id_municipio', 'id_municipio_6']],
                left_on='id_municipio_6_endereco_paciente', right_on='id_municipio_6')
  df = df.rename(columns={'id_municipio': 'id_municipio_endereco_paciente'})

  #-----------------#
  # estabelecimento
  #-----------------#
  
  df = df.merge(munic[['id_municipio', 'id_municipio_6']],
                left_on='id_municipio_6_estabelecimento', right_on='id_municipio_6')
  df = df.rename(columns={'id_municipio': 'id_municipio_estabelecimento'})

  #-----------------#
  # vacinação
  #-----------------#
  
  df['grupo_atendimento_vacina'] = pd.to_numeric(df['grupo_atendimento_vacina'], errors='coerce').astype('string').replace(['0'],'')

  ######transforma caracter especial da variável
  df.loc[(df['dose_vacina'] =="1ª Dose Revacinação " ), 'dose_vacina'] = "1a Dose"
  df.loc[(df['dose_vacina'] =="1ª Dose" ), 'dose_vacina'] = "1a Dose"
  df.loc[(df['dose_vacina'] =="1Âª Dose" ), 'dose_vacina'] = "1a Dose"
  df.loc[(df['dose_vacina'] =="1º Reforço " ), 'dose_vacina'] = "1o Reforço"
  df.loc[(df['dose_vacina'] =="2ª Dose"), 'dose_vacina'] = "2a Dose"
  df.loc[(df['dose_vacina'] =="2Âª Dose" ), 'dose_vacina'] = "2a Dose"
  df.loc[(df['dose_vacina'] =="2ª Dose Revacinação " ), 'dose_vacina'] = "2a Dose Revacinação"
  df.loc[(df['dose_vacina'] =="Dose " ), 'dose_vacina'] = "Dose Única"
  df.loc[(df['dose_vacina'] =="Dose" ), 'dose_vacina'] = "Dose Única"
  df.loc[(df['dose_vacina'] =="3ª Dose" ), 'dose_vacina'] = "3a Dose"
  df.loc[(df['dose_vacina'] =="Única " ), 'dose_vacina'] = "Dose Única"
  df.loc[(df['dose_vacina'] =="Dose Adicional " ), 'dose_vacina'] = "Dose Adicional"
  df.loc[(df['dose_vacina'] =="Dose Inicial " ), 'dose_vacina'] = "Dose Inicial"
  df.loc[(df['dose_vacina'] =="ReforÃ§o" ), 'dose_vacina'] = "Dose Reforço"

  df['data_aplicacao_vacina'] = df['data_aplicacao_vacina'].str[:11]
  
  # df['horario_importacao_rnds'] = df['data_importacao_rnds'].str[11:19]
  # df['data_importacao_rnds']    = df['data_importacao_rnds'].str[:10]

  df = df[['id_documento',
           'id_paciente',
           'idade_paciente',
           'data_nascimento_paciente',
           'sexo_paciente',
           'raca_cor_paciente',
           'id_municipio_endereco_paciente',
           'pais_endereco_paciente',
           'cep_endereco_paciente',
           'nacionalidade_paciente',
           'id_estabelecimento',
           'razao_social_estabelecimento',
           'nome_fantasia_estabelecimento',
           'id_municipio_estabelecimento',
           'grupo_atendimento_vacina',
           'categoria_vacina',
           'lote_vacina',
           'nome_fabricante_vacina',
           'referencia_fabricante_vacina',
           'data_aplicacao_vacina',
           'dose_vacina',
           'codigo_vacina',
           'sistema_origem']]

  df.to_csv("output/microdados/sigla_uf={}/microdados_{}.csv".format(uf,n_chunk),
            index=False)
  
def build_vacinacao(uf, df, n_chunk):
  path = "output/microdados_vacinacao/sigla_uf={}".format(uf)
  os.system(f'mkdir -p {path}')
  df = df[['document_id',
          'paciente_id',
          'estabelecimento_valor',
          'vacina_grupoAtendimento_codigo',
          'vacina_categoria_codigo',
          'vacina_lote',
          'vacina_fabricante_nome',
          'vacina_fabricante_referencia',
          'vacina_dataAplicacao',
          'vacina_descricao_dose',
          'vacina_codigo',
          'sistema_origem']]

  df.columns = ['id_documento',
                'id_paciente',
                'id_estabelecimento',
                'grupo_atendimento',
                'categoria',
                'lote',
                'nome_fabricante',
                'referencia_fabricante',
                'data_aplicacao',
                'dose',
                'vacina',
                'sistema_origem']

  df['grupo_atendimento'] = pd.to_numeric(df['grupo_atendimento'], errors='coerce').astype('string').replace(['0'],'')

  df['data_aplicacao'] = df['data_aplicacao'].str[:11]
  
  ######transforma caracter especial da variável
  df.loc[(df['dose'] =="1ª Dose Revacinação " ), 'dose'] = "1a Dose"
  df.loc[(df['dose'] =="1ª Dose" ), 'dose'] = "1a Dose"
  df.loc[(df['dose'] =="1Âª Dose" ), 'dose'] = "1a Dose"
  df.loc[(df['dose'] =="1º Reforço " ), 'dose'] = "1o Reforço"
  df.loc[(df['dose'] =="2ª Dose"), 'dose'] = "2a Dose"
  df.loc[(df['dose'] =="2Âª Dose" ), 'dose'] = "2a Dose"
  df.loc[(df['dose'] =="2ª Dose Revacinação " ), 'dose'] = "2a Dose Revacinação"
  df.loc[(df['dose'] =="Dose " ), 'dose'] = "Dose Única"
  df.loc[(df['dose'] =="Dose" ), 'dose'] = "Dose Única"
  df.loc[(df['dose'] =="3ª Dose" ), 'dose'] = "3a Dose"
  df.loc[(df['dose'] =="Única " ), 'dose'] = "Dose Única"
  df.loc[(df['dose'] =="Dose Adicional " ), 'dose'] = "Dose Adicional"
  df.loc[(df['dose'] =="Dose Inicial " ), 'dose'] = "Dose Inicial"
  df.loc[(df['dose'] =="ReforÃ§o" ), 'dose'] = "Dose Reforço"


  df = df[['id_documento',
          'id_paciente',
          'id_estabelecimento',
          'grupo_atendimento',
          'categoria',
          'lote',
          'nome_fabricante',
          'referencia_fabricante',
          'data_aplicacao',
          'dose',
          'vacina',
          'sistema_origem']]

  df.to_csv("output/microdados_vacinacao/sigla_uf={}/microdados_vacinacao_{}.csv".format(uf,n_chunk),
            index=False)

def build_paciente(uf, df, munic, n_chunk):
  path = "output/microdados_paciente/sigla_uf_endereco={}".format(uf)
  os.system(f'mkdir -p {path}')
  
  df = df[['paciente_id',
           'paciente_idade',
           'paciente_dataNascimento',
           'paciente_enumSexoBiologico',
           'paciente_racaCor_codigo',
           'paciente_endereco_coIbgeMunicipio',
           'paciente_endereco_coPais',
           'paciente_endereco_cep',
           'paciente_nacionalidade_enumNacionalidade']]
  
  df = df.drop_duplicates()

  df.columns = ['id_paciente',
                'idade',
                'data_nascimento',
                'sexo',
                'raca_cor',
                'id_municipio_6',
                'pais_endereco',
                'cep_endereco',
                'nacionalidade']

  #print(df[df['raca_cor'].isnull()]) # https://stackoverflow.com/questions/47333227/pandas-valueerror-cannot-convert-float-nan-to-integer
  df = df.dropna(subset=['raca_cor']) # dropping the few observations with null information

  df['raca_cor'] = pd.to_numeric(df['raca_cor'], errors='coerce').astype('int64').astype('string')

  df = df.merge(munic[['id_municipio', 'id_municipio_6']],
                on='id_municipio_6')
  df = df.rename(columns={'id_municipio': 'id_municipio_endereco'})

  df = df[['id_paciente',
           'idade',
           'data_nascimento',
           'sexo',
           'raca_cor',
           'id_municipio_endereco',
           'pais_endereco',
           'cep_endereco',
           'nacionalidade']]

  df.to_csv("output/microdados_paciente/sigla_uf_endereco={}/microdados_paciente_{}.csv".format(uf,n_chunk),
            index=False)

def build_estabelecimento(uf, df, munic, n_chunk):
  path = "output/microdados_estabelecimento/sigla_uf={}".format(uf)
  os.system(f'mkdir -p {path}')

  df = df[['estabelecimento_valor',
           'estabelecimento_razaoSocial',
           'estalecimento_noFantasia',
           'estabelecimento_municipio_codigo']]
  
  df = df.drop_duplicates()

  df.columns = ['id_estabelecimento',
                'razao_social',
                'nome_fantasia',
                'id_municipio_6']

  df = df.merge(munic[['id_municipio', 'id_municipio_6']],
                on='id_municipio_6')
  
  df = df[['id_municipio',
           'id_estabelecimento',
           'razao_social',
           'nome_fantasia']]

  df.to_csv("output/microdados_estabelecimento/sigla_uf={}/microdados_estabelecimento_{}.csv".format(uf,n_chunk),
            index=False)