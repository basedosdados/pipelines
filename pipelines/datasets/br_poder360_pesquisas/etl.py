# nivel da observação: id_pesquisa - id_cenario - id_candidato

# match `nome_municipio` para `id_municipio`
# entender o que `condicao` quer dizer

import urllib.request
import json
import pandas as pd
import os
from tqdm import tqdm

os.system('mkdir -p /tmp/data/poder360/')

header = [
        'id_pesquisa',
        'ano',
        'sigla_uf',
        'nome_municipio',
        'cargo',
        'data',
        'data_referencia',
        'instituto',
        'contratante',
        'orgao_registro',
        'numero_registro',
        'quantidade_entrevistas',
        'margem_mais',
        'margem_menos',
        'tipo',
        'turno',
        'tipo_voto',
        'id_cenario',
        'descricao_cenario',
        'id_candidato_poder360',
        'nome_candidato',
        'sigla_partido',
        'condicao',
        'percentual'
        ]

data = pd.DataFrame(columns=header)

for year in tqdm(range(2000,2023)):
        url = 'https://pesquisas.poder360.com.br/web/consulta/fetch?data_pesquisa_de={}-01-01&data_pesquisa_ate={}-12-31&order_column=ano&order_type=asc'.format(year, year)
        req = urllib.request.Request(url, headers={'User-Agent' : "Magic Browser"})
        try:
                response = urllib.request.urlopen(req)
                data_json = json.loads(response.read())
                df = pd.json_normalize(data_json)
                df = df[['pesquisa_id',
                        'ano',
                        'ambito',
                        'cargo',
                        'tipo',
                        'turno',
                        'data_pesquisa',
                        'instituto',
                        'voto_tipo',
                        'cenario_id',
                        'cenario_descricao',
                        'candidatos_id',
                        'candidato',
                        'condicao',
                        'percentual',
                        'data_referencia',
                        'margem_mais',
                        'margem_menos',
                        'contratante',
                        'num_registro',
                        'orgao_registro',
                        'qtd_entrevistas',
                        'partido',
                        'cidade']]
                df.columns = [
                        'id_pesquisa',
                        'ano',
                        'sigla_uf',
                        'cargo',
                        'tipo',
                        'turno',
                        'data',
                        'instituto',
                        'tipo_voto',
                        'id_cenario',
                        'descricao_cenario',
                        'id_candidato_poder360',
                        'nome_candidato',
                        'condicao',
                        'percentual',
                        'data_referencia',
                        'margem_mais',
                        'margem_menos',
                        'contratante',
                        'numero_registro',
                        'orgao_registro',
                        'quantidade_entrevistas',
                        'sigla_partido',
                        'nome_municipio']
                df = df[header]
                df['sigla_uf'] = df['sigla_uf'].str.replace('BR','')
                df['cargo']     = df['cargo'].str.lower()
                df['tipo']      = df['tipo'].str.lower()
                df['tipo_voto'] = df['tipo_voto'].str.lower()
                df['sigla_uf'] = df['sigla_uf'].str.replace('Novo','NOVO')
                df['sigla_uf'] = df['sigla_uf'].str.replace('Patriota','PATRI')
                df['sigla_uf'] = df['sigla_uf'].str.replace('Podemos','PODE')
                df['sigla_uf'] = df['sigla_uf'].str.replace('Progressistas','PP')
                df['sigla_uf'] = df['sigla_uf'].str.replace('Prona','PRONA')
                df['sigla_uf'] = df['sigla_uf'].str.replace('Pros','PROS')
                df['sigla_uf'] = df['sigla_uf'].str.replace('Psol','PSOL')
                df['sigla_uf'] = df['sigla_uf'].str.replace('Rede','REDE')

                data = pd.concat([data,df])
        
        except: #HTTPError:
                print("Erro http em {}".format(year))

data.to_csv('/tmp/data/poder360/microdados_poder360.csv', index=False)

df.columns