# -*- coding: utf-8 -*-
"""
Tasks for br_b3_cotacoes
"""

from prefect import task
import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
from pipelines.datasets.br_bcb_estban.constants import (
    constants as br_b3_cotacoes_constants,
)

from pipelines.utils.utils import (
    to_partitions,
    log,
)
from pipelines.constants import constants

from pipelines.datasets.br_b3_cotacoes.utils import (
    download_and_unzip,
    read_files
)



@task(
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)

def tratamento():
    
    download_and_unzip(
        br_b3_cotacoes_constants.B3_URL,
        br_b3_cotacoes_constants.B3_PATH,
    )

    read_files(
        br_b3_cotacoes_constants.B3_PATH,
    )

    rename = {
    'DataReferencia' : 'data_referencia',
    'CodigoInstrumento' : 'codigo_instrumento',
    'AcaoAtualizacao' : 'acao_atualizacao',
    'PrecoNegocio' : 'preco_negocio',
    'QuantidadeNegociada' : 'quantidade_negociada',
    'HoraFechamento' : 'hora_fechamento',
    'CodigoIdentificadorNegocio' : 'codigo_identificador_negocio',
    'TipoSessaoPregao' : 'tipo_sessao_pregao',
    'DataNegocio' : 'data_negocio',
    'CodigoParticipanteComprador' : 'codigo_participante_comprador',
    'CodigoParticipanteVendedor' : 'codigo_participante_vendedor'
    }

    df.rename(columns=rename, inplace=True)
    df = df.replace(np.nan, '')
    df['codigo_participante_vendedor'] = df['codigo_participante_vendedor'].apply(lambda x: str(x).replace('.0', ''))
    df['codigo_participante_comprador'] = df['codigo_participante_comprador'].apply(lambda x: str(x).replace('.0', ''))
    df['preco_negocio'] = df['preco_negocio'].apply(lambda x: str(x).replace(',', '.'))
    df['data_referencia'] = pd.to_datetime(df['data_referencia'], format='%Y-%m-%d')
    df['data_negocio'] = pd.to_datetime(df['data_negocio'], format='%Y-%m-%d')
    df['preco_negocio'] = df['preco_negocio'].astype(float)
    df['codigo_identificador_negocio'] = df['codigo_identificador_negocio'].astype(str)

    to_partitions(
        df,
        partition_columns=['data_negocio'],
        path=br_b3_cotacoes_constants.B3_PATH_OUTPUT,
    )

    return br_b3_cotacoes_constants.B3_PATH_OUTPUT
