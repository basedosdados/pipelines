from pipelines.utils.utils import run_cloud
from pipelines.datasets.br_me_novo_caged.flows import cagedmov

run_cloud(cagedmov,labels=["basedosdados-dev"])