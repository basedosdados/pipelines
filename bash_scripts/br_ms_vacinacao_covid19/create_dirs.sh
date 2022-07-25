#!/bin/bash
rm -r /tmp/data

declare -a new_dirs=(/tmp/data /tmp/data/br_ms_vacinacao_covid19 /tmp/data/br_ms_vacinacao_covid19/input /tmp/data/br_ms_vacinacao_covid19/microdados /tmp/data/br_ms_vacinacao_covid19/microdados_vacinacao /tmp/data/br_ms_vacinacao_covid19/microdados_paciente /tmp/data/br_ms_vacinacao_covid19/microdados_estabelecimento)

for dir in "${new_dirs[@]}"
do
    mkdir -p $dir
done

tree /tmp/data/