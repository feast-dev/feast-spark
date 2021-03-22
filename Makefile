MVN := mvn ${MAVEN_EXTRA_OPTS}
ROOT_DIR 	:= $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

PROTO_TYPE_SUBDIRS = api
PROTO_SERVICE_SUBDIRS = api

# Make sure env vars are available to submakes
export

# Java

format-java:
	cd spark/ingestion && ${MVN} spotless:apply

lint-java:
	cd spark/ingestion && ${MVN} --no-transfer-progress spotless:check

test-java:
	${MVN} --no-transfer-progress clean verify

# Python

format-python:
	# Sort
	cd ${ROOT_DIR}/python ; isort feast_spark/
	#cd ${ROOT_DIR}/tests/e2e; isort .

	# Format
	cd ${ROOT_DIR}/python; black --target-version py37 feast_spark
	#cd ${ROOT_DIR}/tests/e2e; black --target-version py37 .

install-python-ci-dependencies:
	pip install --no-cache-dir -r python/requirements-ci.txt

compile-protos-python:
	$(MAKE) install-python-ci-dependencies || true
	@$(eval FEAST_PATH=`python -c "import feast; import os; print(os.path.dirname(feast.__file__))"`)
	@$(foreach dir,$(PROTO_TYPE_SUBDIRS),cd ${ROOT_DIR}/protos; python -m grpc_tools.protoc -I. -I$(FEAST_PATH)/protos/ --python_out=../python/ --mypy_out=../python/ feast_spark/$(dir)/*.proto;)
	@$(foreach dir,$(PROTO_SERVICE_SUBDIRS),cd ${ROOT_DIR}/protos; python -m grpc_tools.protoc -I. -I$(FEAST_PATH)/protos/ --grpc_python_out=../python/ feast_spark/$(dir)/*.proto;)
	cd ${ROOT_DIR}/protos; python -m grpc_tools.protoc -I. --python_out=../python/ --grpc_python_out=../python/ --mypy_out=../python/ feast_spark/third_party/grpc/health/v1/*.proto

# Supports feast-dev repo master branch
install-python: compile-protos-python
	cd ${ROOT_DIR}; python -m pip install -e python

lint-python:
	cd ${ROOT_DIR}/python ; mypy feast_spark/ tests/
	cd ${ROOT_DIR}/python ; isort feast_spark/ tests/ --check-only
	cd ${ROOT_DIR}/python ; flake8 feast_spark/ tests/
	cd ${ROOT_DIR}/python ; black --check feast_spark tests
	cd ${ROOT_DIR}/tests; mypy e2e
	cd ${ROOT_DIR}/tests; isort e2e --check-only
	cd ${ROOT_DIR}/tests; flake8 e2e
	cd ${ROOT_DIR}/tests; black --check e2e

test-python:
	pytest --verbose --color=yes python/tests

build-local-test-docker:
	docker build -t feast:local -f infra/docker/tests/Dockerfile .

build-ingestion-jar-no-tests:
	cd spark/ingestion && ${MVN} --no-transfer-progress -Dmaven.javadoc.skip=true -Dgpg.skip -DskipUTs=true -DskipITs=true -Drevision=${REVISION} clean package

build-jobservice-docker:
	docker build -t $(REGISTRY)/feast-jobservice:$(VERSION) -f infra/docker/jobservice/Dockerfile .

push-jobservice-docker:
	docker push $(REGISTRY)/feast-jobservice:$(VERSION)

build-spark-docker:
	docker build -t $(REGISTRY)/feast-spark:$(VERSION) --build-arg VERSION=$(VERSION) -f infra/docker/spark/Dockerfile .

push-spark-docker:
	docker push $(REGISTRY)/feast-spark:$(VERSION)

install-ci-dependencies: install-python-ci-dependencies
