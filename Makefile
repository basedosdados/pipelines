# Get the executables we're using
POETRY=$(shell which poetry)
PYTHON=$(shell poetry run which python)

# `make install`: installs dependencies
.PHONY: install
install:
	$(POETRY) install

# `make install`: installs pre-commit
.PHONY: install_precommit
install_precommit:
	$(POETRY) run pre-commit install --install-hooks
