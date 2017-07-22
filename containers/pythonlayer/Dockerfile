FROM goonalytics:latest
MAINTAINER Ben Levine <ben@goonalytics.io>
RUN pip3 install --upgrade scrapy lxml bs4 google-cloud-core \
  google-cloud-bigquery google-cloud-storage flask elasticsearch avro-python3
COPY resources /goonalytics/resources
COPY python /goonalytics/python
ENV PYTHON_PATH /goonalytics/python/:/tenkyu/tenkyu
CMD ["sh"]
