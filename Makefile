PYTHON = python3
PIP = ${PYTHON} -m pip
PY_TEST = ${PYTHON} -m pytest


.PHONY: default
default: test lint

.PHONY: deps
deps:
	${PIP} install -e .[dev]

.PHONY: test
test:
	${PY_TEST}

.PHONY: coverage
coverage:
	${PY_TEST} --cov-config .coveragerc --cov=./

.PHONY: coverage-html
coverage-html:
	${PY_TEST} --cov-config .coveragerc --cov=./ --cov-report html:htmlcov

.PHONY: lint
lint:
	pylint marshmallow_pyspark

.PHONY: release
release: test
	${PYTHON} setup.py sdist
	twine upload dist/*