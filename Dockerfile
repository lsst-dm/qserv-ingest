ARG REPLICATION_IMAGE
FROM $REPLICATION_IMAGE as ingest-deps
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>
USER qserv
ENV PYTHONPATH=/ingest/python
ENV PATH="/ingest/bin:${PATH}"

FROM ingest-deps 
COPY rootfs /
