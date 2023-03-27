# -*- coding: utf-8 -*-
"""
Tasks for br_anatel_banda_larga_fixa
"""

from prefect import task

import pandas as pd
import numpy as np
import os
import zipfile
import subprocess
from zipfile import ZipFile

@task
def anos_arquivo():
    dict_anos = {
        '2007-2010':['2007','2008','2009','2010'],
        '2011-2012':['2011','2012'],
        '2013-2014':['2013','2014'],
        '2015-2016':['2015','2016'],
        '2017-2018':['2017','2018'],
        '2019-2020' : ['2019', '2020'],
        '2021' : ['2021'],
        '2022' : ['2022'],
        '2023' : ['2023']
    }
    return dict_anos

@task
def particionamento(df,ano):
    for ano_dict in anos_arquivo()[ano]:
        for mes in range(1, 13):
            for sigla_uf in df['sigla_uf'].unique():
                path_arquivos = '/content/drive/Shareddrives/Base dos Dados - Geral/Dados/Conjuntos/br_anatel_banda_larga_fixa/particionamento/ano={ano_dict}/mes={mes}/sigla_uf={sigla_uf}'
                particao = f'{path_arquivos}'
                if not os.path.exists(particao):
                    os.makedirs(particao)
                df_particao = df[(df['ano'] == ano) & (df['mes'] == mes) & (df['sigla_uf'] == sigla_uf)].copy()
                df_particao.drop(['ano', 'mes', 'sigla_uf'], axis = 1, inplace=True)
                particao = f'{path_arquivos}' + '/microdados.csv'
                df_particao.to_csv(particao, sep=';', index=False, encoding='utf-8')

@task
def tratamento():

    url =  'https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip'
    command = ['wget', url]
    subprocess.call(command)

    pasta = '/content/www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos'
    banda_larga = os.path.join(pasta, 'acessos_banda_larga_fixa.zip')
    path_particionamento = '/content/drive/Shareddrives/Base dos Dados - Geral/Dados/Conjuntos/br_anatel_banda_larga_fixa/particionamento' 

    anos = ['2007-2010', '2011-2012', '2013-2014', '2015-2016', '2017-2018', '2019-2020', '2021', '2022', '2023']
    with ZipFile(banda_larga) as z:
        for ano in anos:
            try:
                with z.open(f'Acessos_Banda_Larga_Fixa_{ano}.csv') as f:
                    df = pd.read_csv(f, sep=';', encoding='utf-8')
                    df.rename(columns={'Ano': 'ano','Mês':'mes', 'Grupo Econômico':'grupo_economico', 'Empresa':'empresa',
                                    'CNPJ':'cnpj', 'Porte da Prestadora':'porte_empresa', 'UF':'sigla_uf', 'Município':'municipio',
                                    'Código IBGE Município':'id_municipio', 'Faixa de Velocidade':'velocidade', 'Tecnologia':'tecnologia',
                                    'Meio de Acesso':'transmissao', 'Acessos':'acessos', 'Tipo de Pessoa': 'pessoa'}, inplace=True)


                    # organização das variáveis
                    df.drop(['grupo_economico', 'municipio'], axis=1, inplace=True)
                    df = df[['ano', 'mes', 'sigla_uf', 'id_municipio', 'cnpj', 'empresa', 'porte_empresa', 'tecnologia', 'transmissao', 'velocidade', 'acessos']]
                    df['acessos_total'] = df.groupby(['ano', 'mes', 'sigla_uf', 'id_municipio', 'cnpj', 'empresa', 'porte_empresa', 'tecnologia', 'transmissao', 'velocidade'])['acessos'].transform(np.sum)
                    #após ordenamento das observações, se mantém somente 1 linha que identifique as observações

                    df.sort_values(['ano', 'mes', 'sigla_uf', 'id_municipio', 'cnpj', 'empresa', 'porte_empresa', 'tecnologia', 'transmissao', 'velocidade'], inplace=True)

                    df.drop('acessos', axis=1, inplace=True)

                    df.rename(columns={'acessos_total':'acessos'}, inplace=True)

                    particionamento(df,ano)
            except Exception:
                print("Erro em ano{}".format(ano))
                
    return path_particionamento
