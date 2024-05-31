FROM apache/airflow:2.8.3

USER root

RUN sudo apt update && apt-get install software-properties-common -y

RUN sudo apt-get install -y build-essential binutils libproj-dev gdal-bin libgdal-dev proj-bin geos-bin

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

USER airflow

RUN mkdir .config

RUN mkdir .config/earthengine

COPY config/credentials /home/airflow/.config/earthengine/credentials

COPY requirements.txt .

RUN pip install earthengine-api google-auth google-auth-oauthlib google-auth-httplib2 geemap gdal==3.6.2

RUN pip install -r requirements.txt
