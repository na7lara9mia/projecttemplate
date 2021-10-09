# Documentation
PACKAGE=$(BIN)/$(PACKAGE_DIR)
APIDOC_OPTIONS = -d 1 --no-toc --separate --force --private

.PHONY: doc test coverage notebook

help:
	@echo "======================================================================================="
	@echo "                             PSA DATA SCIENCE TEMPLATE"
	@echo "======================================================================================="
	@echo "Avalaible commands:"
	@echo "clean       : remove cache, install lock, jupyter install"
	@echo "distclean   : remove egg info file"
	@echo "rename      : change the name of the package (ask for old name and new package name)"
	@echo "install     : install all packages specified in conf/requirements.txt"
	@echo "upgrade-pip : upgrade pip"
	@echo "notebook    : install dependencies and launch jupyter notebook (ask for notebook password)"
	@echo "test        : launch test set, code coverage"
	@echo "doc         : generate documentation"
	@echo "format      : reformat code using PEP8 and PEP256 rules"
	@echo "describe    : print project environment infos"

clean:
	@echo Cleaning $(current_dir)...
	find . -name __pycache__ -print | xargs rm -rf 2>/dev/null
	find . -name '*.pyc' -delete
	rm -rf .pytest_cache/
	rm -rf install
	rm -rf .install_jupyter

distclean: clean
	rm -rf dist build *.egg-info

rename:
	sh ./script/change_name.sh

.venv3:
	rm install || true &&\
	/soft/python3/bin/virtualenv ./.venv3 &&\
	source conf/app-profile.sh &&\
	pip install $$PIP_OPTIONS --upgrade pip pypandoc

install: .venv3 conf/requirements.txt
	source conf/app-profile.sh && \
	pip install $$PIP_OPTIONS -r conf/requirements.txt &&\
	touch install

upgrade-pip:
	source conf/app-profile.sh && \
	pip install $$PIP_OPTIONS --upgrade pip

.install_jupyter: install
	source conf/app-profile.sh &&\
	pip install $$PIP_OPTIONS jupyter &&\
	jupyter nbextension enable --py --sys-prefix widgetsnbextension &&\
	touch .install_jupyter

notebook:  .install_jupyter
	source conf/app-profile.sh && \
	sh ./script/notebook.sh start

test: install
	source conf/app-profile.sh &&\
	cd $$BIN/ &&\
	python -m pytest --cov=. $$PACKAGE_DIR test

doc:
	source conf/app-profile.sh &&\
	rm doc/source/generated/*  || true
	rm -r doc/build/html/* || true
	export PYTHONPATH=$$(pwd):$$PYTHONPATH &&\
	source conf/app-profile.sh &&\
	sphinx-apidoc $(APIDOC_OPTIONS) -o doc/source/generated/ $(PACKAGE) &&\
	sphinx-build -M html doc/source doc/build

format:
	source conf/app-profile.sh &&\
	black $(PACKAGE) &&\
	black $$BIN/test

describe:
	sh ./script/describe.sh
