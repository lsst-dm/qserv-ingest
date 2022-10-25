FROM golang:1.17.6-bullseye AS dbbench
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y bash curl git mariadb-client openssh-server && \
    rm -rf /var/lib/apt/lists/*
RUN go get github.com/lsst-dm/dbbench@163e978def488c6600c22fffe3ea80c4713f9642

FROM python:3.10.2-bullseye as ingest-deps
LABEL org.opencontainers.image.authors="fabrice.jammes@in2p3.fr"

RUN pip3 install --upgrade pip==22.0.4


# libmariadb3 and mariadb-python are required for SQLalchemy to work with mysql-proxy+mariadb:1:10.6.5
# see https://lsstc.slack.com/archives/C996604NR/p1643105168095900 for additional informations
RUN apt-get update && \
    apt-get install -y ca-certificates libmariadb3 \
    python3-dev default-libmysqlclient-dev build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY rootfs/usr/local/share/ca-certificates /usr/local/share/ca-certificates
RUN update-ca-certificates

COPY --from=dbbench /go/bin/dbbench /usr/local/bin

RUN python3 -m pip install jsonpath-ng==1.5.2 \
    mariadb==1.0.9 \
    mysqlclient==2.1.0 PyYAML==5.3.1  \
    requests==2.25.1 retry==0.9.2 SQLAlchemy==1.4.31

#USER qserv

# FIXME use a secret below:
RUN mkdir /root/.lsst && touch /root/.lsst/qserv
ENV PYTHONPATH=/ingest/python
ENV PATH="/ingest/bin:${PATH}"

FROM ingest-deps
COPY rootfs/ingest /ingest

# TODO build an image with static analysis tools
# TODO run 'black rootfs --line-length 110'
# RUN python3 -m pip install mypy==0.950

