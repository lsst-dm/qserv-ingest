ARG REPLICATION_IMAGE
FROM $REPLICATION_IMAGE as ingest-deps
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>
# RUN echo "deb http://ftp.debian.org/debian stretch-backports main" >> /etc/apt/sources.list
USER root
RUN yum -y install curl jq vim && yum clean all
USER qserv
ENV PYTHONPATH=/ingest/python
ENV PATH="/ingest/bin:${PATH}"

FROM ingest-deps 
COPY rootfs /
