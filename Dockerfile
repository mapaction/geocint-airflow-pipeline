FROM apache/airflow:2.8.3

USER root

RUN sudo apt update && apt-get install software-properties-common -y

RUN sudo apt-get install -y build-essential binutils libproj-dev gdal-bin libgdal-dev proj-bin geos-bin

# Add the Cloud SDK distribution URI as a package source
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# Import the Google Cloud Platform public key
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# Update the package list and install the Google Cloud SDK
RUN apt-get update && apt-get install -y google-cloud-sdk

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

USER airflow
COPY requirements.txt .

RUN pip install -r requirements.txt
