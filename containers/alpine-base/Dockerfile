FROM alpine:latest
MAINTAINER Ben Levine <ben@goonalytics.io>
RUN echo http://dl-cdn.alpinelinux.org/alpine/edge/testing >> /etc/apk/repositories && \
  apk --no-cache -U update upgrade && \
  apk add --no-cache python3 python3-dev ca-certificates gcc g++ libffi-dev libxml2-dev libxslt-dev openssl-dev openblas-dev && \
  ln -s /usr/include/locale.h /usr/include/xlocale.h && pip3 install --upgrade pip
CMD ["sh"]
