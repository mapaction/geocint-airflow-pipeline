# MapAction Airflow pipeline 

This is a data pipeline using Apache Airflow to rebuild the 
[geocint mapaction pipeline](https://github.com/mapaction/geocint-mapaction/). 

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

## Start clean build 

1. Ensure you have the AIRFLOW_UID with this command 

### Make directories
- `mkdir -p ./dags ./logs ./plugins ./config`
### Get AIRFLOW_UID
- `echo -e "AIRFLOW_UID=$(id -u)" > .env`

### Append WebDAV variables
- `echo "WEBDAV_HOSTNAME=" >> .env`
- `echo "WEBDAV_LOGIN=" >> .env`
- `echo "WEBDAV_PASSWORD=" >> .env`

2. Clear all the the docker container using `docker system prune --all`
Note this will remove all containers fron the suytem if you just wan to remove airflow containers, you can use `docker container prune`
3. Run `docker compose up airflow-init`
5. Run `docker compose up`
6. Run `docker ps` to get the container ID of the airflow-worker container and copy it.
7. Enter the container using the following `docker exec -it -u root <continer_id> bash`
8. Create a group ID with the same code as the AIRFLOW_UID using `sudo groupadd <AIRFLOW_UID>`
9. Give default user a password `sudo passwd default <password>`
10. Add default user to sudoers `sudo usermod -aG sudo default`
11. Add default user to the group `sudo usermod -aG <AIRFLOW_UID> default `
12. Add peremission to /opt/airflow directory using `sudo chown -R default:<ARIFLOW_UID> /opt/airflow/`
13. The default location for the webserver is [http://localhost:8080](http://localhost:8080). The default username and password