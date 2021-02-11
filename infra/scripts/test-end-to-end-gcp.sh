#!/usr/bin/env bash

export DISABLE_SERVICE_FIXTURES=1
export GIT_TAG=$PULL_PULL_SHA
export GIT_REMOTE_URL=https://github.com/feast-dev/feast-spark.git

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

kubectl run -n "$NAMESPACE" -i ci-test-runner  \
    --pod-running-timeout=5m \
    --restart=Never \
    --image="${DOCKER_REPOSITORY}/feast-ci:latest" \
    --env="FEAST_TELEMETRY=false" \
    -- bash -c \
"mkdir src && cd src && git clone --recursive ${GIT_REMOTE_URL} && cd feast-spark && " \
"git config remote.origin.fetch '+refs/pull/*:refs/remotes/origin/pull/*' && git fetch -q && git checkout ${GIT_TAG} && " \
"make install-python && python -m pip install -qr tests/requirements.txt && " \
"pytest -v tests/e2e/ --staging-path gs://feast-templocation-kf-feast/ --core-url feast-release-feast-core:6565 " \
"--serving-url feast-release-feast-online-serving:6566 --job-service-url gcp-test-feast-jobservice:6568 " \
"--kafka-brokers 10.128.0.103:9094 --bq-project kf-feast"

#su -p postgres -c "PATH=$PATH HOME=/tmp pytest -v tests/e2e/ \
#      --feast-version develop --env=gcloud --dataproc-cluster-name feast-e2e \
#      --dataproc-project kf-feast --dataproc-region us-central1 \
#        \
#      --with-job-service \
#      --redis-url 10.128.0.105:6379 --redis-cluster  \
#      "
