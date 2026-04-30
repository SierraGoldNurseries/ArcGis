# SGN Structures Online Map - Light GitHub Version

This is the lightweight GitHub Pages version of the SGN structures map.

## Files

- `index.html` - the online Leaflet map
- `data/structures_polygons.geojson` - combined polygon layer
- `data/structures_points.geojson` - generated centroid point layer
- `data/layer_inventory.csv` - layer inventory/report

The previous full package included per-layer GeoJSON and CSV exports. This light package removes those extra files so GitHub Pages is easier to upload and publish.

## Run locally

```bash
python3 -m http.server 8000
```

Then open:

```text
http://localhost:8000
```

## Publish on GitHub Pages

1. Create a new GitHub repo.
2. Upload these files and folders to the repo root.
3. Go to **Settings > Pages**.
4. Set source to **Deploy from branch**.
5. Choose `main` and `/root`.
6. Save.

