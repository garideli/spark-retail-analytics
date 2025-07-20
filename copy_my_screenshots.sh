#!/bin/bash

echo "Copying screenshots to docs/screenshots directory..."

# Copy raw data screenshot
cp "/var/folders/c5/ybp1wl2s3896gfn80wrxfwm80000gn/T/TemporaryItems/NSIRD_screencaptureui_i3Mr69/Screenshot 2025-07-20 at 2.27.16 AM.png" docs/screenshots/raw-data-output.png && echo "✓ Raw data screenshot copied" || echo "✗ Failed to copy raw data screenshot"

# Copy order items screenshot
cp "/var/folders/c5/ybp1wl2s3896gfn80wrxfwm80000gn/T/TemporaryItems/NSIRD_screencaptureui_hfOcpK/Screenshot 2025-07-20 at 2.30.13 AM.png" docs/screenshots/order-items-output.png && echo "✓ Order items screenshot copied" || echo "✗ Failed to copy order items screenshot"

# Copy transformed data screenshot
cp "/var/folders/c5/ybp1wl2s3896gfn80wrxfwm80000gn/T/TemporaryItems/NSIRD_screencaptureui_d9HLqI/Screenshot 2025-07-20 at 2.31.25 AM.png" docs/screenshots/transformed-data-output.png && echo "✓ Transformed data screenshot copied" || echo "✗ Failed to copy transformed data screenshot"

echo ""
echo "Done! Check the status above."