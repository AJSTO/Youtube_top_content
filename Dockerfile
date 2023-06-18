# To build the Docker image:
# docker build -t extending_airflow_yt:latest .

FROM apache/airflow:latest

COPY requirements.txt /requirements.txt

USER root

RUN apt-get update \
    && apt-get -y install gcc \
    && apt-get install -y libgdal-dev g++ --no-install-recommends \
    && apt-get clean -y

USER ${AIRFLOW_UID}

RUN pip install --user --upgrade pip \
    && pip install --no-cache-dir --user -r /requirements.txt \
    && pip install protobuf==3.19.0
