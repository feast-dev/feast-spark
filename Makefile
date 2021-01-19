MVN := mvn ${MAVEN_EXTRA_OPTS}
ROOT_DIR 	:= $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

format-java:
	${MVN} spotless:apply

lint-java:
	${MVN} --no-transfer-progress spotless:check

install-python-ci-dependencies:
	pip install --no-cache-dir -r feast_spark/requirements-ci.txt

lint-python:
	mypy feast_spark/
	isort feast_spark/ --check-only
	flake8 feast_spark/
	black --check feast_spark 

