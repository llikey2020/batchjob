ARG CI_REGISTRY
FROM ${CI_REGISTRY}/planetrover/infrastructure/debian:stable

LABEL "Name"="batch-job"
LABEL "Version"="0.1.0-alpha"
EXPOSE 80

RUN mkdir -p /opt/batch-job/manifests

COPY ./batch-service /opt/batch-job/batch-service
CMD ["/opt/batch-job/batch-service"]
