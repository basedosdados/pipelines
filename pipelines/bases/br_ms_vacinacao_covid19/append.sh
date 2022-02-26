#!/bin/bash
# see:https://stackoverflow.com/questions/24641948/merging-csv-files-appending-instead-of-merging

# to run use: bash append [csvs_prefix] [output file name]
OutFileName="/tmp/data/br_ms_vacinacao_covid19/input/$2.csv"
i=0
for filename in /tmp/data/br_ms_vacinacao_covid19/input/$1*.csv; do
 if [ "$filename"  != "$OutFileName" ] ;
 then
   if [[ $i -eq 0 ]] ; then
      head -1  "$filename" >   "$OutFileName"
   fi
   tail -n +2  "$filename" >>  "$OutFileName"
   i=$(( $i + 1 ))
 fi
done
