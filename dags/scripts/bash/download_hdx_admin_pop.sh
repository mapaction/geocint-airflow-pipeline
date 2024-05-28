#!/bin/bash

# This script downloads tabular data about population for country code from $1
# it uses manually created file static_data/hdx_population_tabular.json
# inside json there are: url to download layer, name for result dataset

country_code=$(basename "${1%%.*}")
out_dir="data/output/country_extractions/${country_code}/223_popu"

mkdir -p $out_dir

# find country_code and download tabular data
while IFS=$'\t' read -r url file_name
do
    rm -f $out_dir/$file_name.csv
    wget -q -O $out_dir/$file_name.csv $url
done < <( cat static_data/hdx_admin_pop_urls.json | jq -r --arg cnt $country_code '(.[] | select( .country_code | test( $cnt; "i" )) | .val | .[] | [ .download_url, .ma_file_name ]) | @tsv' )