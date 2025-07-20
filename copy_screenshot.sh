#!/bin/bash

# This script will help you copy the screenshot

echo "Copying screenshot to docs/screenshots..."

# Try different approaches to copy the file
if [ -f ~/Desktop/"Screenshot 2025-07-20 at 2.27.03 AM.png" ]; then
    cp ~/Desktop/"Screenshot 2025-07-20 at 2.27.03 AM.png" ~/projects/spark-retail-analytics/docs/screenshots/raw-data-output.png
    echo "✓ Screenshot copied successfully!"
else
    echo "❌ Screenshot not found on Desktop"
    echo "Please manually copy your screenshot to:"
    echo "  ~/projects/spark-retail-analytics/docs/screenshots/raw-data-output.png"
fi

# Verify the copy worked
if [ -f ~/projects/spark-retail-analytics/docs/screenshots/raw-data-output.png ]; then
    echo "✓ File exists at: docs/screenshots/raw-data-output.png"
    ls -la ~/projects/spark-retail-analytics/docs/screenshots/
else
    echo "❌ File was not copied successfully"
fi