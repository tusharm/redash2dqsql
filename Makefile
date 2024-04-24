all: help

header:
	@echo "========================================================================"
	@echo "====== redash2dqsql ===================================================="
	@echo "========================================================================"
	@echo ""

help: header
	@echo "Make targets"
	@echo " 	build:  Build the package"
	@echo " 	setup:  Setup the development environment"
	@echo " 	test: 󰙨 Run the tests"
	@echo " 	clean: 󰗩 Clean the build files"
	@echo " 	distclean: 󰛌 Clean the build files and the virtual environment"
	@echo " 	help: 󰋗 Show this help message"
	@echo ""

build: header
	@echo "Building the package..."
	@venv/bin/python setup.py build install
	@echo "Done."

setup: header
	@echo "Setting up the development environment..."
	if [ ! -d "venv" ]; then python3 -m venv venv; fi
	@venv/bin/pip install -r requirements.txt
	@echo "Done."
	@echo "To activate the virtual environment, run: source venv/bin/activate"

test: header
	@echo "Running the tests..."
	@venv/bin/python -m unittest discover -s tests
	@echo "Done."

clean: header
	@echo "Cleaning the build files..."
	@rm -rf build dist redash2dqsql.egg-info
	@echo "Done."

distclean: header
	@echo "Cleaning the build files and the virtual environment..."
	@rm -rf build dist redash2dqsql.egg-info venv
	@echo "Done."

.PHONY: build setup test clean distclean help