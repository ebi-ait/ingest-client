TOOL_NAME := geo_to_hca
SRC_FILES := $(shell find $(TOOL_NAME) -name \*.py -print)

MODULES=ingest tests
.PHONY: lint test unit-tests

lint:
	flake8 $(MODULES) *.py --ignore=E501,E731,W503

test: lint unit-tests

unit-tests:
	time python -m unittest discover --start-directory tests --verbose

dist: setup.py $(SRC_FILES)
	python setup.py sdist

publish: dist
	twine  upload \
		--verbose \
		--comment "release $(TOOL_NAME)" \
		dist/*

clean:
	rm -rf dist/ output/ *.egg-info/
