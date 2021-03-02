ARG BASE_IMAGE
FROM $BASE_IMAGE as deps
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

RUN apk update
ENV NGINX_ROOT_DIR=/www

FROM deps 
COPY data /usr/share/nginx/html
