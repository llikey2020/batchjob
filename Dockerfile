FROM debian

LABEL "Name"="batch-job"
LABEL "Version"="0.1.0-alpha"
EXPOSE 80

COPY ./sparkctl /opt/sparkctl/sparkctl
ENV PATH=${PATH}:/opt/sparkctl

RUN mkdir -p /opt/batch-job/manifests

COPY ./batch-service /opt/batch-job/batch-service
CMD ["/opt/batch-job/batch-service"]
