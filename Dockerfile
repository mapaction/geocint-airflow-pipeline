FROM apache/airflow:2.8.3

USER root

RUn sudo apt update && apt-get install software-properties-common -y

RUN sudo apt-get install -y build-essential binutils libproj-dev gdal-bin libgdal-dev proj-bin geos-bin

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

USER airflow

COPY requirements.txt .

RUN pip install -r requirements.txt
