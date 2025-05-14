#!/bin/bash

# Função para baixar e processar os arquivos para uma UF específica
baixar_uf() {
    UF="$1"
    URL="https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios_preliminares/malha_com_atributos/setores/shp/UF/$UF/${UF}_Malha_Preliminar_2022.zip"
    ROOT="/tmp/data/"

    echo "Baixando arquivo para $UF..."
    curl -o "$ROOT/input/${UF}_Malha_Preliminar_2022.zip" "$URL"

}


UFS=("AC" "AL" "AP" "AM" "BA" "CE" "DF" "ES" "GO" "MA" "MT" "MS" "MG" "PA" "PB" "PR" "PE" "PI" "RJ" "RN" "RS" "RO" "RR" "SC" "SP" "SE" "TO")


for UF in "${UFS[@]}"; do
    baixar_uf "$UF"
done

echo "Concluído!"
