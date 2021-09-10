ARG BASE_IMAGE
FROM golang:1.16-alpine AS dbbench
RUN apk update && apk upgrade && \
    apk add --no-cache bash curl git mysql-client openssh
RUN go get github.com/lsst-dm/dbbench@163e978def488c6600c22fffe3ea80c4713f9642
RUN wget $(curl -s https://api.github.com/repos/mikefarah/yq/releases/latest | grep browser_download_url | grep linux_amd64 | cut -d '"' -f 4) -O /usr/bin/yq &&  chmod +x /usr/bin/yq

FROM $BASE_IMAGE as ingest-deps
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

RUN apk update \
    && apk add --virtual build-deps gcc musl-dev \
    && apk add --no-cache mariadb-dev mariadb-connector-c \
    && pip install mysqlclient==2.0.1 \
    && apk del build-deps mariadb-dev \
    && apk add ca-certificates \
    && rm -rf /var/cache/apk/*

COPY rootfs/usr/local/share/ca-certificates /usr/local/share/ca-certificates
RUN update-ca-certificates

RUN pip3 install --upgrade pip==21.1.2

RUN pip3 install PyYAML==5.3.1 jsonpath-ng==1.5.2 \
    requests==2.25.1 SQLAlchemy==1.3.20

COPY --from=dbbench /go/bin/dbbench /usr/local/bin
COPY --from=dbbench /usr/bin/yq /usr/bin

#USER qserv
# FIXME use a secret below:
RUN mkdir /root/.lsst && touch /root/.lsst/qserv
ENV PYTHONPATH=/ingest/python
ENV PATH="/ingest/bin:${PATH}"

FROM ingest-deps
COPY rootfs/ingest /ingest
