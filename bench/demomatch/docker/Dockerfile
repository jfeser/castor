FROM postgres:10

RUN apt-get update \
    && apt-get -y install libssl1.0.2 \
    && apt-get -y install pgloader

COPY config.sh db-names.txt /docker-entrypoint-initdb.d/
