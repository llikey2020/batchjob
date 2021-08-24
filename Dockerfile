FROM debian

COPY ./sparkctl /opt/sparkctl/sparkctl
ENV PATH=${PATH}:/opt/sparkctl

RUN mkdir -p /opt/batch-job/manifests

COPY ./batch-service /opt/batch-job/batch-service
ENTRYPOINT ["/opt/batch-job/batch-service"]
