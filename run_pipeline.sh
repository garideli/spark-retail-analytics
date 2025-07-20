#!/bin/bash

# Run the PySpark retail analytics pipeline

echo "Starting Retail Analytics Pipeline..."

# Create output directories if they don't exist
mkdir -p data/output
mkdir -p data/processed
mkdir -p logs

# Run the pipeline with sample display
python main.py \
    --input-path data/raw \
    --output-path data/output \
    --log-level INFO \
    --generate-reports \
    --show-samples

echo "Pipeline execution completed!"