#!/usr/bin/env bash

set -e

export DISABLE_FEAST_SERVICE_FIXTURES=1
export DISABLE_SERVICE_FIXTURES=1
export GIT_TAG=${PULL_PULL_SHA:-${PULL_BASE_SHA}}
export DOCKER_REPOSITORY=gcr.io/kf-feast
export JOBSERVICE_HELM_VALUES=infra/scripts/helm/k8s-jobservice.yaml

test -z ${GCLOUD_PROJECT} && GCLOUD_PROJECT="kf-feast"
test -z ${GCLOUD_REGION} && GCLOUD_REGION="us-central1"
test -z ${GCLOUD_NETWORK} && GCLOUD_NETWORK="default"
test -z ${GCLOUD_SUBNET} && GCLOUD_SUBNET="default"
test -z ${KUBE_CLUSTER} && KUBE_CLUSTER="feast-e2e-dataflow"

gcloud auth activate-service-account --key-file ${GOOGLE_APPLICATION_CREDENTIALS}
gcloud -q auth configure-docker

gcloud config set project ${GCLOUD_PROJECT}
gcloud config set compute/region ${GCLOUD_REGION}
gcloud config list

gcloud container clusters get-credentials ${KUBE_CLUSTER} --region ${GCLOUD_REGION} --project ${GCLOUD_PROJECT}

source infra/scripts/k8s-common-functions.sh

NAMESPACE="sparkop-e2e"

k8s_cleanup "feast-release" "$NAMESPACE"
k8s_cleanup "js" "$NAMESPACE"

kubectl delete sparkapplication --all -n $NAMESPACE
kubectl delete scheduledsparkapplication --all -n $NAMESPACE

wait_for_image "${DOCKER_REPOSITORY}" feast-jobservice "${GIT_TAG}"
wait_for_image "${DOCKER_REPOSITORY}" feast-spark "${GIT_TAG}"

sed s/\$\{IMAGE_TAG\}/${JOBSERVICE_GIT_TAG:-$GIT_TAG}/g infra/scripts/helm/k8s-jobservice.tpl.yaml > $JOBSERVICE_HELM_VALUES

helm_install "js" "${DOCKER_REPOSITORY}" "${GIT_TAG}" "$NAMESPACE" \
  --set 'feast-online-serving.application-override\.yaml.feast.stores[0].type=REDIS' \
  --set 'feast-online-serving.application-override\.yaml.feast.stores[0].name=online' \
  --set 'feast-online-serving.application-override\.yaml.feast.stores[0].config.host=feast-release-redis-master' \
  --set 'feast-online-serving.application-override\.yaml.feast.stores[0].config.port=6379' \

make install-python
python -m pip install -qr tests/requirements.txt
pytest -v tests/e2e/ --env k8s \
	--staging-path gs://feast-templocation-kf-feast/ \
	--core-url feast-release-feast-core:6565 \
  --serving-url feast-release-feast-online-serving:6566 \
  --job-service-url js-feast-jobservice:6568 \
  --kafka-brokers feast-release-kafka-headless:9092 --bq-project kf-feast --feast-version dev
