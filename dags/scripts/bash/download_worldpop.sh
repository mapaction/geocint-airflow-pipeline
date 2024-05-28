#!/bin/bash

# this script downloads worldPop tif files per country
# input params: country_json_file resolution(100m|1km)
#           static-data/countries/ARG.json 100m
# outputput: data/output/country_extractions/arg/223_popu/arg_popu_pop_ras_s1_worldpop_pp_2020UNadj_100m.tif
#           static-data/countries/ARG.json 1km
# outputput: data/output/country_extractions/arg/223_popu/arg_popu_pop_ras_s1_worldpop_pp_PopDensity_2020UNad.tif
# if URL is wrong or if failed to download - this script will fail and target will also fail

set -e

country_geojson_filename=$(basename "$1")
country_code="${country_geojson_filename%.*}"
# convert country_code to upper for url
country_code_upper=$(echo "$country_code" | tr '[:lower:]' '[:upper:]')
# if second param is empty then use default "100m"
resolution=${2:-100m}

# create directory if not exist,
# "223_popu" is from /static-data/directories/directories.csv
mkdir -p data/output/country_extractions/$country_code/223_popu

if [ "${resolution}" = "100m" ]; then
    curl -s -f -o "data/output/country_extractions/${country_code}/223_popu/${country_code}_popu_pop_ras_s1_worldpop_pp_2020unadj_100m.tif" \
        "https://data.worldpop.org/GIS/Population/Global_2000_2020/2020/${country_code_upper}/${country_code}_ppp_2020_UNadj.tif"
else
    curl -s -f -o "data/output/country_extractions/${country_code}/223_popu/${country_code}_popu_pop_ras_s1_worldpop_pp_popdensity_2020unad.tif" \
        "https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km_UNadj/2020/${country_code_upper}/${country_code}_pd_2020_1km_UNadj.tif"
fi

exit 0