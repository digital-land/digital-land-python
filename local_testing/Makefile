.PHONY: help init setup-dirs setup-spec check-spec list run fast compare test clean clean-all

PYTHON := venv/bin/python3
PIP := venv/bin/pip
SPEC_DIR := ../specification

help:
	@echo "Title Boundary Pipeline"
	@echo ""
	@echo "  make init              First time setup (dirs + venv + spec check)"
	@echo "  make setup-dirs        Create all required directories"
	@echo "  make setup-spec        Clone specification files from GitHub"
	@echo "  make check-spec        Check if specification files exist"
	@echo "  make list              List available Local Authorities"
	@echo "  make run LA=Name       Process a Local Authority"
	@echo "  make fast LA=Name      Process with DuckDB + Parquet"
	@echo "  make compare LA=Name   Run original + Polars for comparison"
	@echo "  make test              Test all module imports"
	@echo "  make clean             Remove generated data"
	@echo "  make clean-all         Remove data + venv"
	@echo ""
	@echo "Examples:"
	@echo "  make init                              # Complete setup"
	@echo "  make setup-spec                        # Clone specification if missing"
	@echo "  make run LA=Buckinghamshire LIMIT=100"
	@echo "  make fast LA=\"East Sussex\""
	@echo "  make compare LA=Buckinghamshire LIMIT=1000"

setup-dirs:
	@mkdir -p raw extracted converted output reports cache pipeline
	@echo "✅ Created directories: raw/ extracted/ converted/ output/ reports/ cache/ pipeline/"

setup-spec:
	@if [ -d "$(SPEC_DIR)" ]; then \
		echo "✅ Specification already exists at $(SPEC_DIR)"; \
	else \
		echo "📥 Cloning specification from GitHub..."; \
		cd .. && git clone https://github.com/digital-land/specification.git; \
		if [ -d "$(SPEC_DIR)" ]; then \
			echo "✅ Specification cloned successfully"; \
			echo "   Files: $$(ls -1 $(SPEC_DIR)/*.csv 2>/dev/null | wc -l | tr -d ' ') CSV files"; \
		else \
			echo "❌ Failed to clone specification"; \
			exit 1; \
		fi \
	fi

check-spec:
	@if [ -d "$(SPEC_DIR)" ]; then \
		echo "✅ Specification found at $(SPEC_DIR)"; \
		echo "   Files: $$(ls -1 $(SPEC_DIR)/*.csv 2>/dev/null | wc -l | tr -d ' ') CSV files"; \
	else \
		echo "❌ Specification not found at $(SPEC_DIR)"; \
		echo ""; \
		echo "Run 'make setup-spec' to clone automatically, or:"; \
		echo "  cd ../"; \
		echo "  git clone https://github.com/digital-land/specification.git"; \
		echo ""; \
		exit 1; \
	fi

init: setup-dirs venv setup-spec
	@echo "✅ Setup complete - ready to run pipeline"

venv:
	@python3 -m venv venv
	@$(PIP) install -q --upgrade pip
	@$(PIP) install -q polars duckdb requests
	@$(PIP) install -q -e ..
	@echo "  ✓ Installed digital-land-python in editable mode"

list: venv
	@$(PYTHON) main.py --list

run: venv
	@test -n "$(LA)" || (echo "Error: make run LA=Name"; exit 1)
	@$(PYTHON) main.py --la "$(LA)" $(if $(LIMIT),--limit $(LIMIT))

fast: venv
	@test -n "$(LA)" || (echo "Error: make fast LA=Name"; exit 1)
	@$(PYTHON) main.py --la "$(LA)" --use-duckdb --use-parquet $(if $(LIMIT),--limit $(LIMIT))

compare: venv
	@test -n "$(LA)" || (echo "Error: make compare LA=Name"; exit 1)
	@$(PYTHON) main.py --la "$(LA)" --compare $(if $(LIMIT),--limit $(LIMIT))

test: venv
	@$(PYTHON) -c "from cli import CLI; from file_downloader import FileDownloader; from gml_extractor import GMLExtractor; from gml_converter import GMLConverter; from pipeline_config import PipelineConfig; from pipeline_runner import PipelineRunner; from pipeline_report import PipelineReport; print('✅ All modules OK')"

clean:
	@rm -rf raw/* extracted/* converted/* output/* reports/*
	@echo "✅ Data cleaned"

clean-all: clean
	@rm -rf venv/ cache/*
	@echo "✅ All cleaned"
