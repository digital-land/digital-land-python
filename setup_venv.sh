#!/bin/bash
# Automated virtual environment setup with required dependencies

set -e  # Exit on error

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/venv"

echo "Setting up Python virtual environment..."

# Create venv if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment at $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
else
    echo "Virtual environment already exists at $VENV_DIR"
fi

# Activate venv
source "$VENV_DIR/bin/activate"

# Upgrade pip, setuptools, and wheel
echo "Upgrading pip, setuptools, and wheel..."
pip install --upgrade pip setuptools wheel

# Install dependencies in order
echo "Installing development dependencies..."
pip install -e .

# Install testing dependencies if they exist
if [ -f "$PROJECT_DIR/requirements-test.txt" ]; then
    echo "Installing test requirements..."
    pip install -r requirements-test.txt
fi

# Install local development requirements if they exist
if [ -f "$PROJECT_DIR/requirements-local.txt" ]; then
    echo "Installing local development requirements..."
    pip install -r requirements-local.txt
fi

echo "✓ Virtual environment setup complete!"
echo "To activate the environment in the future, run:"
echo "  source $VENV_DIR/bin/activate"
