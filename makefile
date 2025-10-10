MKDOCS_CONFIG := ./docs/mkdocs.yml
SRC := ./src/proctor_schedule
DATA_DIR := ./data/
RAW := $(DATA_DIR)/raw
INTERIM := $(DATA_DIR)/interim
PROCESSED := $(DATA_DIR)/processed

# Get the current project version from pyproject.toml
VERSION := $(shell grep '^version =' pyproject.toml | sed 's/version = "\(.*\)"/\1/')

# The archive target copies output directories to a folder with today's date
OUTPUT_DIRS := logs models reports $(INTERIM) $(PROCESSED)
CURRENT_DATE := $(shell date +'%Y-%m-%d')
ARCHIVE_DIR := ./archive/$(CURRENT_DATE)

.PHONY: all \
	docs \
	env format lint \
	clean clean-output clean-pycache archive

all: env

env:
	@echo "Syncing virtual environment with uv..."
	@uv sync

docs:
	@echo "Serving documentation..."
	@mkdocs serve -f $(MKDOCS_CONFIG) -o

format:
	@echo "Formatting code with ruff..."
	@ruff format $(SRC)

lint:
	@echo "Linting code with ruff..."
	@ruff check $(SRC)

clean-output:
	@for dir in $(OUTPUT_DIRS); do \
		echo "Cleaning directory: $$dir..."; \
		find $$dir -type f ! -name ".gitkeep" -exec rm -f {} +; \
	done

clean-pycache:
	@echo "Cleaning pycache files..."
	@find $$src -name "*.pyc" -exec rm -f {} +;
	@find . -type d -name "__pycache__" -delete

clean: clean-output clean-pycache

archive:
	@# If an archive already exists for today, we add a suffix to the current date for this archive.
	@new_archive_dir=$(ARCHIVE_DIR); \
	suffix=2; \
	while [ -d "$$new_archive_dir" ]; do \
		new_archive_dir="$(ARCHIVE_DIR)_$$suffix"; \
		suffix=$$((suffix + 1)); \
	done; \
	mkdir -p $$new_archive_dir; \
	echo "Archiving output folders to $$new_archive_dir..."; \
	for dir in $(OUTPUT_DIRS) $(RAW); do \
		echo "Copying $$dir..."; \
		mkdir -p $$new_archive_dir/$$dir/; \
		cp -r $$dir/* $$new_archive_dir/$$dir/; \
	done

release: all
	@echo "Creating GitHub release for v$(VERSION)..."
	@zip -r raw_data.zip $(RAW) \
		--exclude=$(RAW)/.gitkeep
	@gh release create "v$(VERSION)" \
		--title "Release v$(VERSION)" \
		--notes "Our raw data is attached to this release in \`raw_data.zip\`. The contents of this archive should be extracted into \`data/raw\`." \
		--target main \
		--prerelease \
		raw_data.zip
	@rm -f raw_data.zip
