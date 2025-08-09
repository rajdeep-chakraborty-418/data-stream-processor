DIR_SRC_FILES = $(shell find . -type f -name "*.py" \
-not -path "./idea/*" \
-not -path "./build/*" \
-not -path "./.venv/*" \
-not -path "./mock/*" \
)
PYLINT_THRESHOLD= 9
CMD_PYLINT_OPTIONS = --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" --output-format=parseable --reports=y --score=y --fail-under=${PYLINT_THRESHOLD}
CMD_PYTEST_OPTIONS = -ra -vv --tb=short --cache-clear --color=yes --show-capture=no --strict-markers
COVERAGE_THRESHOLD = 90

unit_test:
	python3 -m pytest -vv -s --cache-clear test/main/*

lint:
	python3 -m pylint ${CMD_PYLINT_OPTIONS} ${DIR_SRC_FILES}

coverage:
	python3 -m coverage run --source=src -m pytest ${CMD_PYTEST_OPTIONS}

coverage_report:
	python3 -m coverage html --fail-under=${COVERAGE_THRESHOLD}

wheel:
	python3 setup.py bdist_wheel

build: unit_test lint lint coverage coverage_report wheel
