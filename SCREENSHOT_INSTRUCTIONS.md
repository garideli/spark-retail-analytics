# Screenshot Instructions

Since the temporary screenshot files get moved/deleted quickly by macOS, please follow these steps:

## Option 1: Save Screenshots Directly
When taking screenshots, save them directly to the project folder:
1. Press `Cmd + Shift + 5` to take a screenshot
2. Click "Options" and choose "Other Location..."
3. Navigate to `/Users/drupa/projects/spark-retail-analytics/docs/screenshots/`
4. Name them:
   - `raw-data-output.png` (for Customers, Products, Orders data)
   - `order-items-output.png` (for Order Items data)
   - `transformed-data-output.png` (for Sales Summary and Customer Behavior)

## Option 2: Use Desktop First
1. Save screenshots to Desktop first
2. Then copy them:
```bash
# From Desktop to project
cp ~/Desktop/your-screenshot1.png docs/screenshots/raw-data-output.png
cp ~/Desktop/your-screenshot2.png docs/screenshots/order-items-output.png
cp ~/Desktop/your-screenshot3.png docs/screenshots/transformed-data-output.png
```

## Option 3: Drag and Drop
1. Open Finder
2. Navigate to `/Users/drupa/projects/spark-retail-analytics/docs/screenshots/`
3. Drag your screenshot files directly into this folder
4. Rename them to match the expected names

## Verify Screenshots
After copying, verify they exist:
```bash
ls -la docs/screenshots/
```

You should see:
- raw-data-output.png
- order-items-output.png
- transformed-data-output.png

Once these files are in place, your README.md will automatically display them on GitHub!