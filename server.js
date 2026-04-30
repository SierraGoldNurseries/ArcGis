#!/usr/bin/env node
/*
  Optional local/production static server for precompressed GeoJSON.

  Run from ArcGis-main after node scripts/build-map-data.js:
    npm install express compression
    node server.js

  Then open:
    http://localhost:3000
*/

const express = require('express');
const compression = require('compression');
const fs = require('fs');
const path = require('path');

const app = express();
const ROOT = process.cwd();
const PORT = process.env.PORT || 3000;

app.use(compression());

app.get('*.geojson', (req, res, next) => {
  const requested = path.join(ROOT, decodeURIComponent(req.path));
  const accept = req.headers['accept-encoding'] || '';

  if (accept.includes('br') && fs.existsSync(requested + '.br')) {
    res.setHeader('Content-Encoding', 'br');
    res.setHeader('Content-Type', 'application/geo+json; charset=utf-8');
    res.setHeader('Vary', 'Accept-Encoding');
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
    return fs.createReadStream(requested + '.br').pipe(res);
  }

  if (accept.includes('gzip') && fs.existsSync(requested + '.gz')) {
    res.setHeader('Content-Encoding', 'gzip');
    res.setHeader('Content-Type', 'application/geo+json; charset=utf-8');
    res.setHeader('Vary', 'Accept-Encoding');
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
    return fs.createReadStream(requested + '.gz').pipe(res);
  }

  next();
});

app.use(express.static(ROOT, {
  etag: true,
  lastModified: true,
  maxAge: '1h',
  setHeaders(res, filePath) {
    if (filePath.endsWith('.geojson')) res.setHeader('Content-Type', 'application/geo+json; charset=utf-8');
    if (/data[\\/]optimized/.test(filePath)) res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
  }
}));

app.listen(PORT, () => console.log(`SGN map server running at http://localhost:${PORT}`));
