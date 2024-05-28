# Proof of Concept - MapAction Airflow pipeline 

This is a proof of concept data pipeline using Apache Airflow to rebuild the 
[geocint mapaction pipeline](https://github.com/mapaction/geocint-mapaction/). 


## TODO 

- [In progress] Implement more pipeline steps
- [x] Test deploying to AWS  
  - [x] Make AWS VPC (creation in progress. It's slow...) 
  - [x] Make S3 Bucket
  - [x] Make MWAA
    - Outcome - MWAA was very simple to setup, but may be too expensive for a non-profit. 
      Likely around £5500/yr _as a lower bound_. 
- [ ] Test deploying to GCP 
- [ ] Test deploying to dagster 
- [In progress] Estimate cloud costs 
- [x] Connect to S3 / GCS for data output
- [ ] Set up CI/CD
- [ ] Generate slow / non changing data (particularly elevation) 
- [ ] Find mechanism to check if country boundary has changed (hash / checksum?) for 
conditional logic on whether to re-generate the country's elevation dataset (the slow step)
- [ ] Fix bug where dynamic pipeline generation seems to always get "moz" (even for Afganistan?!)
- [ ] 250m grid is 3Gb and takes around 5 mins to download on a fast internet connection
  - Things to try: 
  - [ ] Upload to S3 and see how fast that is to download from 



## Geocint <> Airflow POC mapping 

This table summarises the Geocint Makefile [recipie's prerequisites](https://github.com/mapaction/geocint-mapaction/blob/main/Makefile). This have been 
remade / mapped in the POC pipeline with the same names, and this is currently working
(although only 3 steps have actually been implemented.)

|   Stage name  |   Prerequisites   |   What it does / scripts it calls   |
|---|---|---|
|   Export country   |   NA - PHONY  |   Extracts polygon via osmium    Osm data import (import from ? To protocol buffer format)   Mapaction data table (upload to Postgres in new table)   Map action export (creates .shp and .json files for a list of ? [countries? Counties? Other?])   Mapaction upload cmf (uploads shp+tiff and geojson+tiff to s3, via ?cmf)  |
|   All   |   Dev   |     |
|   Dev  |   upload_datasets_all, upload_cmf_all, create_completeness_report  |   slack_message.py  |
|   .  |     |     |
|   upload_datasets_all   |   datasets_ckan_descriptions  |   mapaction_upload_dataset.sh  - Creates a folder and copies all .shp, .tif and .json files into it.   mapaction_upload_dataset.sh  - Zips it   Creates a folder called “/data/out/country_extractions/<country_name>” in S3, and copies the zip folder into it.   |
|   upload_cmf_all  |   cmf_metadata_list_all  |   See above by export country   |
|   datasets_ckan_descriptions  |   datasets_all  |   mapaction_build_dataset_description.sh -   |
|   datasets_all  |   ne_10m_lakes, ourairports, worldports, wfp_railroads, global_power_plant_database, ne_10m_rivers_lake_centerlines, ne_10m_populated_places, ne_10m_roads, healthsites, ocha_admin_boundaries, mapaction_export, worldpop1km, worldpop100m, elevation, download_hdx_admin_pop  |     |
|   ne_10m_lakes  |     |     |
|   ourairports  |     |     |
|   worldports  |     |     |
|   wfp_railroads  |     |     |
|   global_power_plant_database  |     |     |
|   ne_10m_rivers_lake_centerlines  |     |     |
|   ne_10m_populated_places  |     |     |
|   ne_10m_roads  |     |     |
|   Health sites  |     |     |
|   ocha_admin_boundaries  |     |     |
|   mapaction_export  |     |     |
|   worldpop1km  |     |     |
|   Elevation  |     |     |
|   download_hdx_admin_pop  |     |     |

## Notes

- Currently trying AWS's [managed service](https://docs.aws.amazon.com/mwaa/latest/userguide/what-is-mwaa.html). 
- See [here](https://github.com/aws/aws-mwaa-local-runner/issues/157) for how to install non-python dependencies (e.g. gdal) 
- If you get "Dag import errors", the actual errors are in the logs directory/volume
- To add new python packages
  1. Stop all docker containers 
  2. Remove containers and images with `docker container prune` and delete the docker image for the airflow worker. 
  3. Update your requirements.txt file with `pip freeze > requirements.txt`
  4. Run `docker compose up`

## Quickstart

1. Install docker and docker compose 
2. Follow the Airflow steps for using docker compose [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).
3. Note, you may need to force recreating images, e.g. `docker compose up --force-recreate`
4. The default location for the webserver is [http://localhost:8080](http://localhost:8080). The default username and password is `airflow`. 