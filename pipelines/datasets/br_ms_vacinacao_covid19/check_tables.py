import pandas as pd 
import numpy as np
from glob import glob
import os 

df=pd.read_csv('/tmp/data/br_ms_vacinacao_covid19/output/microdados_estabelecimento.csv',
dtype={'id_municipio':str})

print(np.unique([k[:2] for k in df.id_municipio.unique()]))

csvs = glob('/tmp/data/br_ms_vacinacao_covid19/output/*csv')
for csv in csvs:
    df=pd.read_csv(csv, nrows=10)
    print(csv+':')
    print(df)

for csv in csvs:
    os.system('''
    wc -l {0}
    '''.format(csv))