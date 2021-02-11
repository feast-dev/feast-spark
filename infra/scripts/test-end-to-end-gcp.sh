#!/usr/bin/env bash

export DISABLE_SERVICE_FIXTURES=1
export GIT_TAG=$PULL_PULL_SHA
export MAVEN_OPTS="-Dmaven.repo.local=/tmp/.m2/repository -DdependencyLocationsEnabled=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=3 -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false"
export MAVEN_CACHE="gs://feast-templocation-kf-feast/.m2.2020-11-17.tar"

test -z ${GCLOUD_PROJECT} && GCLOUD_PROJECT="kf-feast"
test -z ${GCLOUD_REGION} && GCLOUD_REGION="us-central1"
test -z ${GCLOUD_NETWORK} && GCLOUD_NETWORK="default"
test -z ${GCLOUD_SUBNET} && GCLOUD_SUBNET="default"
test -z ${KUBE_CLUSTER} && KUBE_CLUSTER="feast-e2e-dataflow"

test -z ${DOCKER_REPOSITORY} && DOCKER_REPOSITORY="gcr.io/kf-feast"

infra/scripts/download-maven-cache.sh --archive-uri ${MAVEN_CACHE} --output-dir /tmp
apt-get update && apt-get install -y redis-server postgresql libpq-dev

source infra/scripts/k8s-common-functions.sh

# Prepare gcloud sdk
gcloud auth activate-service-account --key-file ${GOOGLE_APPLICATION_CREDENTIALS}
gcloud -q auth configure-docker

gcloud config set project ${GCLOUD_PROJECT}
gcloud config set compute/region ${GCLOUD_REGION}
gcloud config list

gcloud container clusters get-credentials ${KUBE_CLUSTER} --region ${GCLOUD_REGION} --project ${GCLOUD_PROJECT}

# Install components via helm
helm_install "gcp-test" "${DOCKER_REPOSITORY}" "${GIT_TAG}" "default"

# Build ingestion jar
make build-ingestion-jar-no-tests REVISION=develop

python -m pip install --upgrade pip setuptools wheel
make install-python
python -m pip install -qr tests/requirements.txt
export FEAST_TELEMETRY="False"

su -p postgres -c "PATH=$PATH HOME=/tmp pytest -v tests/e2e/ \
      --feast-version develop --env=gcloud --dataproc-cluster-name feast-e2e \
      --dataproc-project kf-feast --dataproc-region us-central1 \
      --staging-path gs://feast-templocation-kf-feast/ \
      --with-job-service \
      --redis-url 10.128.0.105:6379 --redis-cluster --kafka-brokers 10.128.0.103:9094 \
      --bq-project kf-feast"
