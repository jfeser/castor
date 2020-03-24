FROM postgres:11

RUN apt-get update && \
  apt-get install -y git build-essential
COPY 00_tpch_setup.sh 01_indexes.sh 02_sample.sh /docker-entrypoint-initdb.d/
