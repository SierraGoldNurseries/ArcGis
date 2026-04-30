# SGN Structures Online Map

This is the lightweight GitHub Pages version of the SGN structures map.

## Files

```text
index.html
import.html
data/structures_polygons.geojson
data/structures_points.geojson
data/layer_inventory.csv
```

## Pages

- `index.html` = published structure map with satellite/street basemap toggle, polygons, generated points, and labels.
- `import.html` = browser import page for GeoJSON, CSV, and zipped shapefiles. It can generate points and polygons from supported export files.

## GitHub Pages setup

1. Upload the contents of this folder to the root of your GitHub repo.
2. Go to **Settings > Pages**.
3. Set **Source** to **Deploy from a branch**.
4. Select `main` and `/ (root)`.
5. Save.

## Notes

The original ArcGIS package contained polygon layers, not native point layers. The point layer in `data/structures_points.geojson` was generated from the polygon geometry so each structure has a clickable/searchable point marker.

The map labels use a smart label field:

- Full names for larger/simple polygons.
- Abbreviations for smaller polygons or names likely to clutter the map.
- Four Bays labels are abbreviated as `FB01`, `FB02`, etc.

## Import page limits

Static GitHub Pages cannot directly extract Esri `.ppkx` or File Geodatabase `.gdb` project files in the browser because those need ArcGIS Pro/ArcPy or a server-side FileGDB reader.

Use `import.html` for:

- GeoJSON
- JSON FeatureCollection
- CSV with latitude/longitude
- CSV grouped point rows that should become polygons
- zipped shapefiles

For `.ppkx` or `.gdb`, first export from ArcGIS Pro to GeoJSON or shapefile ZIP, then import that file on `import.html`.
