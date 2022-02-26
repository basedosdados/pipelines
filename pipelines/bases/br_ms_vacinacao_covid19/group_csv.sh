#!/bin/bash

ufs_repeated=($(ls /tmp/data/br_ms_vacinacao_covid19/input/ | egrep -o '_[a-z]+_' | egrep -o '[a-z]+'))
# ids=(aa ab aa ac aa ad)

ufs_folder=($(echo "${ufs_repeated[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))

ufs=()
for i in "${ufs_folder[@]}"
do
    ufs+=("dados_$i,${i^^}")
done

for i in "${ufs[@]}"
do IFS=","
    set -- $i
    bash append.sh $1 $2
done