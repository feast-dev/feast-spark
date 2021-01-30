MVN := mvn ${MAVEN_EXTRA_OPTS}
ROOT_DIR 	:= $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

# Make sure env vars are available to submakes
export

format-java:
	cd spark/ingestion && ${MVN} spotless:apply

format-python:
	# Sort
	cd ${ROOT_DIR}/python ; isort feast_spark/
	#cd ${ROOT_DIR}/tests/e2e; isort .

	# Format
	cd ${ROOT_DIR}/python; black --target-version py37 feast_spark
	#cd ${ROOT_DIR}/tests/e2e; black --target-version py37 .

lint-java:
	cd spark/ingestion && ${MVN} --no-transfer-progress spotless:check

install-python-ci-dependencies:
	pip install --no-cache-dir -r python/requirements-ci.txt

lint-python:
	cd ${ROOT_DIR}/python ; mypy feast_spark/
	cd ${ROOT_DIR}/python ; isort feast_spark/ --check-only
	cd ${ROOT_DIR}/python ; flake8 feast_spark/
	cd ${ROOT_DIR}/python ; black --check feast_spark

build-local-test-docker:
	docker build -t feast:local -f infra/docker/tests/Dockerfile .

build-ingestion-jar-no-tests:
	cd spark/ingestion && ${MVN} --no-transfer-progress -Dmaven.javadoc.skip=true -Dgpg.skip -DskipUTs=true -DskipITs=true -Drevision=${REVISION} clean package

build-metrics-jar:
	cd spark/metrics && ${MVN} --no-transfer-progress -Dmaven.javadoc.skip=true -Dgpg.skip -Drevision=${REVISION} clean package

build-jobservice-docker:
	docker build -t $(REGISTRY)/feast-jobservice:$(VERSION) -f infra/docker/jobservice/Dockerfile .

push-jobservice-docker:
	docker push $(REGISTRY)/feast-jobservice:$(VERSION)

install-python: install-python-ci-dependencies
	python -m pip install -e python

install-ci-dependencies: install-python-ci-dependencies

# Forward all other build-X and push-X targets to the Makefile that knows how to build docker
# containers
build-%:
	cd deps/feast && $(MAKE) $@

push-%:
	cd deps/feast && $(MAKE) $@
