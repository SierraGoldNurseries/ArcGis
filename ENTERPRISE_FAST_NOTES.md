# Enterprise Fast Map Update

This full repository ZIP includes every original file from ArcGis-main plus the enterprise-fast map changes.

## Replaced / added files
- `index.html`
- `scripts/build-map-data.js`
- `scripts/map-data-worker.js`
- `server.js`
- `package.json`
- `.gitattributes`
- `ENTERPRISE_FAST_NOTES.md`

## Run
```bash
cd ArcGis-main
npm install
node scripts/build-map-data.js
npm start
```
Open `http://localhost:3000`.

## Performance behavior
- Starts at 4793 Garden Hwy / Yuba City area.
- Loads simplified core block outlines first.
- Does not load chemicals by default.
- Does not load irrigation by default.
- Irrigation loads only when enabled and zoomed in.
- Irrigation pipe size is shown by color/line width, not map text labels.
- Tree names / varieties are not labeled by default.
- Selected block detail loads only after clicking a block.
- GeoJSON parsing uses `scripts/map-data-worker.js` so large data parsing happens off the main UI thread.
- `scripts/build-map-data.js` creates optimized GeoJSON, compressed `.gz` / `.br` files, simplified core blocks, irrigation lines, block detail files, and PMTiles if `tippecanoe` is installed.
