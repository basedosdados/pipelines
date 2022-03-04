#!/bin/bash

cwd=$(pwd)
output_data=/tmp/data/br_ms_vacinacao_covid19/output/
cp bash_scripts/append.sh $output_data
cd $output_data
folder=($1)
filename=$(echo $folder | tr "/" " " | awk '{print $NF}')
ufs_folder=($(ls $folder | egrep -o '[A-Z]+'))

ufs=()
for i in "${ufs_folder[@]}"
do
    ufs+=("${folder}/,${folder}/$filename")
done

for i in "${ufs[@]}"
do IFS=","
    set -- $i
    bash append.sh $1 $2
done
cd ${folder}
mv $filename.csv ../
cd ../
filepath=$(readlink -f $filename.csv)
rm -r $folder
rm append.sh
cd $cwd
echo $filepath

