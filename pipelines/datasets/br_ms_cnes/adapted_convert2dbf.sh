#!/bin/bash

# Clone blast-dbf and build it
git clone https://github.com/eaglebh/blast-dbf
cd blast-dbf
make
blast=$(readlink -f blast-dbf)
echo $blast
cd ..

# Set path to the directory with .dbc files
files=("$@")

# Iterate over the provided file paths
for file in "${files[@]}"
do
    # Check if the file has a .dbc extension
    if [[ $file == *.dbc ]]; then
        # Execute blast-dbf on the file
        $blast $file "${file/.dbc/.dbf}"
    else
        echo "Skipping non-DBC file: $file"
    fi
done