MVN := mvn ${MAVEN_EXTRA_OPTS}
ROOT_DIR 	:= $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

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

build-java-no-tests:
	cd spark/ingestion && ${MVN} --no-transfer-progress -Dmaven.javadoc.skip=true -Dgpg.skip -DskipUTs=true -DskipITs=true -Drevision=${REVISION} clean package
