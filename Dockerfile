ARG REPLICATION_IMAGE
FROM $REPLICATION_IMAGE
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

# RUN echo "deb http://ftp.debian.org/debian stretch-backports main" >> /etc/apt/sources.list

USER root
RUN yum -y install curl jq vim && yum clean all
USER 0

COPY rootfs /
