# SGN Map App v11 Full Replacement

This package includes:

- Modern collapsed left layer menu
- Restored grouped color shading for structures such as High Tunnels, Shade House, Can Yard, Four Bays, etc.
- Project defaults for Structures/Yards, Chemical Locations, Irrigation, Owl Boxes, Trees, Prune North/South, and Blocks
- Vercel import page for uploading .ppkx files
- GitHub Actions extractor workflow that rebuilds combined GeoJSON for all projects in data/raw

## Vercel environment variables

Set these in Vercel Project Settings > Environment Variables:

```env
GITHUB_OWNER=sierragoldnurseries
GITHUB_REPO=ArcGis
GITHUB_BRANCH=main
GITHUB_TOKEN=your_fine_grained_github_token
ADMIN_PIN=your_private_admin_pin
```

## Important upload note

Vercel serverless uploads can be too small for large .ppkx files. The import page includes two modes:

1. Backend mode: uses /api/upload and Vercel env token; good for small files.
2. Direct GitHub mode: use for large files such as Trees or Irrigation. It uploads from the browser to GitHub without going through Vercel body limits.

After upload, GitHub Actions runs `tools/extract_project.py` and writes:

- data/combined_polygons.geojson
- data/combined_points.geojson
- data/structures_polygons.geojson
- data/structures_points.geojson
- data/map_config.json
- data/project_manifest.json
- data/layer_inventory.csv
