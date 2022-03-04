from pipelines.utils import run_local
from pipelines.bases.br_ibge_ipca.flows import br_ibge_ipca_mes_brasil

if __name__ == "__main__":
    run_local(br_ibge_ipca_mes_brasil)
