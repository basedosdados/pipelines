from pipelines.utils.utils import run_local
from functools import reduce
from pipelines.ibge.br_ibge_ipca15 import (
br_ibge_ipca15_mes_categoria_brasil, br_ibge_ipca15_mes_categoria_rm, br_ibge_ipca15_mes_categoria_municipio, br_ibge_ipca15_mes_brasil
)

from pipelines.ibge.br_ibge_ipca import (
br_ibge_ipca_mes_categoria_brasil, br_ibge_ipca_mes_categoria_rm, br_ibge_ipca_mes_categoria_municipio, br_ibge_ipca_mes_brasil
)

from pipelines.ibge.br_ibge_inpc import (
br_ibge_inpc_mes_categoria_brasil, br_ibge_inpc_mes_categoria_rm, br_ibge_inpc_mes_categoria_municipio, br_ibge_inpc_mes_brasil
)

from pipelines.cvm.br_cvm_oferta_publica_distribuicao import br_cvm_ofe_pub_dis_dia
from pipelines.cvm.br_cvm_administradores_carteira import (br_cvm_adm_car_res, br_cvm_adm_car_pes_fis, br_cvm_adm_car_pes_jur)

flows_ipca15 = [br_ibge_ipca15_mes_categoria_brasil, br_ibge_ipca15_mes_categoria_rm, br_ibge_ipca15_mes_categoria_municipio, br_ibge_ipca15_mes_brasil]

flows_ipca = [br_ibge_ipca_mes_categoria_brasil, br_ibge_ipca_mes_categoria_rm, br_ibge_ipca_mes_categoria_municipio, br_ibge_ipca_mes_brasil]

flows_inpc = [br_ibge_inpc_mes_categoria_brasil, br_ibge_inpc_mes_categoria_rm, br_ibge_inpc_mes_categoria_municipio, br_ibge_inpc_mes_brasil]

flows_cvm = [br_cvm_ofe_pub_dis_dia, br_cvm_adm_car_res, br_cvm_adm_car_pes_fis, br_cvm_adm_car_pes_jur]

flows = reduce(lambda x,y: x+y, [flows_ipca,flows_inpc,flows_ipca15, flows_cvm])


if __name__ == "__main__":
    for flow in flows_inpc:
        run_local(flow)
