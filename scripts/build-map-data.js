#!/usr/bin/env node
/*
  Enterprise map-data build step for ArcGis-main.

  What it creates:
    data/optimized/core-blocks.geojson
    data/optimized/core-blocks-full.geojson
    data/optimized/irrigation-lines.geojson
    data/optimized/block-details/<projectKey>.geojson
    .gz and .br compressed copies for every optimized GeoJSON

  Optional PMTiles/vector tile output:
    If tippecanoe is installed, this also attempts to create:
      data/optimized/tiles/heavy-detail.pmtiles

  Run from ArcGis-main folder:
    node scripts/build-map-data.js
*/

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const childProcess = require('child_process');

const ROOT = process.cwd();
const MAP_DIR = path.join(ROOT, 'data', 'map');
const MANIFEST_PATH = path.join(MAP_DIR, 'manifest.json');
const OUT_DIR = path.join(ROOT, 'data', 'optimized');
const DETAIL_DIR = path.join(OUT_DIR, 'block-details');
const TILE_DIR = path.join(OUT_DIR, 'tiles');
const TMP_DIR = path.join(OUT_DIR, '_tmp');

const CORE_KEEP_KEYS = new Set([
  'id', 'name', 'Name', 'NAME', 'label', 'Label', 'LABEL',
  'block', 'Block', 'BLOCK', 'crop', 'Crop', 'CROP',
  'variety', 'Variety', 'VARIETY', 'acres', 'Acres', 'ACRES',
  '_project', '_project_key', '_layer', '_layer_display', '_source_path', '_pipe_size'
]);

function mkdirp(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function readJson(filePath) {
  const text = fs.readFileSync(filePath, 'utf8');
  if (text.startsWith('version https://git-lfs.github.com/spec/')) {
    const err = new Error('Git LFS pointer file, real GeoJSON not downloaded: ' + filePath);
    err.code = 'GIT_LFS_POINTER';
    throw err;
  }
  return JSON.parse(text);
}

function writeJson(filePath, value) {
  mkdirp(path.dirname(filePath));
  fs.writeFileSync(filePath, JSON.stringify(value));
  compressFile(filePath);
}

function compressFile(filePath) {
  const buf = fs.readFileSync(filePath);
  fs.writeFileSync(filePath + '.gz', zlib.gzipSync(buf, { level: 9 }));
  fs.writeFileSync(filePath + '.br', zlib.brotliCompressSync(buf, {
    params: {
      [zlib.constants.BROTLI_PARAM_QUALITY]: 11,
      [zlib.constants.BROTLI_PARAM_SIZE_HINT]: buf.length
    }
  }));
}

function normalizedPath(p) {
  return p.replace(/\\/g, '/').toLowerCase();
}

function isTreeLike(file, project) {
  const s = normalizedPath(`${project.key} ${project.name} ${file.path} ${file.layerName || ''} ${file.layerKey || ''}`);
  return s.includes('tree') || s.includes('spaces') || s.includes('exportfeatures') || s.includes('chandler') || s.includes('rx1') || s.includes('vx211') || s.includes('clonal');
}

function isChemical(project, file) {
  const s = normalizedPath(`${project.key} ${project.name} ${file.path} ${file.layerName || ''}`);
  return s.includes('chemical') || s.includes('pesticide') || s.includes('fertilizer') || s.includes('propane') || s.includes('gasoline') || s.includes('diesel') || s.includes('acid');
}

function isIrrigation(project, file) {
  const s = normalizedPath(`${project.key} ${project.name} ${file.path} ${file.layerName || ''} ${file.layerKey || ''}`);
  return s.includes('irrigation') || s.includes('pipe') || s.includes('water') || s.includes('pump') || s.includes('filter') || s.includes('well');
}

function isCoreBlockPolygon(project, file) {
  if (file.type !== 'polygon') return false;
  if (isChemical(project, file)) return false;
  if (isIrrigation(project, file)) return false;
  if (isTreeLike(file, project)) return false;
  const s = normalizedPath(`${project.key} ${project.name} ${file.path} ${file.layerName || ''} ${file.layerKey || ''}`);
  if (s.includes('label')) return false;
  return s.includes('block') || s.includes('polygon') || s.includes('orchard') || project.defaultVisible;
}

function pipeSizeFromText(text) {
  const s = normalizedPath(text).replace(/[_-]/g, ' ');
  const wordMap = {
    'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5, 'six': 6,
    'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10, 'twelve': 12,
    'fifteen': 15, 'eighteen': 18, 'twenty': 20
  };
  let m = s.match(/(\d+(?:\.\d+)?)\s*(?:in|inch|inches|\")/i);
  if (m) return Number(m[1]);
  for (const [word, value] of Object.entries(wordMap)) {
    const re = new RegExp(`\\b${word}\\s+(?:in|inch|inches|pipe)\\b`, 'i');
    if (re.test(s)) return value;
  }
  return null;
}

function cleanProps(props, extra = {}) {
  const out = { ...extra };
  for (const [k, v] of Object.entries(props || {})) {
    if (CORE_KEEP_KEYS.has(k) || /name|label|block|crop|acre|variety/i.test(k)) {
      if (v !== null && v !== undefined && String(v).length < 160) out[k] = v;
    }
  }
  return out;
}

function ringArea(ring) {
  let area = 0;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [x1, y1] = ring[j];
    const [x2, y2] = ring[i];
    area += (x1 * y2 - x2 * y1);
  }
  return Math.abs(area / 2);
}

function sqDist(a, b) {
  const dx = a[0] - b[0];
  const dy = a[1] - b[1];
  return dx * dx + dy * dy;
}

function sqSegDist(p, p1, p2) {
  let x = p1[0];
  let y = p1[1];
  let dx = p2[0] - x;
  let dy = p2[1] - y;
  if (dx !== 0 || dy !== 0) {
    const t = ((p[0] - x) * dx + (p[1] - y) * dy) / (dx * dx + dy * dy);
    if (t > 1) { x = p2[0]; y = p2[1]; }
    else if (t > 0) { x += dx * t; y += dy * t; }
  }
  dx = p[0] - x;
  dy = p[1] - y;
  return dx * dx + dy * dy;
}

function simplifyDPStep(points, first, last, sqTolerance, simplified) {
  let maxSqDist = sqTolerance;
  let index = -1;
  for (let i = first + 1; i < last; i++) {
    const sq = sqSegDist(points[i], points[first], points[last]);
    if (sq > maxSqDist) {
      index = i;
      maxSqDist = sq;
    }
  }
  if (index !== -1) {
    if (index - first > 1) simplifyDPStep(points, first, index, sqTolerance, simplified);
    simplified.push(points[index]);
    if (last - index > 1) simplifyDPStep(points, index, last, sqTolerance, simplified);
  }
}

function simplifyRing(ring, tolerance = 0.000015) {
  if (!Array.isArray(ring) || ring.length <= 8) return ring;
  const closed = ring[0][0] === ring[ring.length - 1][0] && ring[0][1] === ring[ring.length - 1][1];
  const pts = closed ? ring.slice(0, -1) : ring.slice();
  const sqTolerance = tolerance * tolerance;
  const filtered = [pts[0]];
  let prev = pts[0];
  for (let i = 1; i < pts.length; i++) {
    if (sqDist(pts[i], prev) > sqTolerance) {
      filtered.push(pts[i]);
      prev = pts[i];
    }
  }
  if (filtered.length < 4) return ring;
  const simplified = [filtered[0]];
  simplifyDPStep(filtered, 0, filtered.length - 1, sqTolerance, simplified);
  simplified.push(filtered[filtered.length - 1]);
  if (closed) simplified.push(simplified[0]);
  return simplified.length >= 4 ? simplified : ring;
}

function simplifyGeometry(geometry, tolerance) {
  if (!geometry) return geometry;
  if (geometry.type === 'Polygon') {
    return { ...geometry, coordinates: geometry.coordinates.map(r => simplifyRing(r, tolerance)) };
  }
  if (geometry.type === 'MultiPolygon') {
    return { ...geometry, coordinates: geometry.coordinates.map(poly => poly.map(r => simplifyRing(r, tolerance))) };
  }
  if (geometry.type === 'LineString') {
    return { ...geometry, coordinates: simplifyRing(geometry.coordinates, tolerance) };
  }
  if (geometry.type === 'MultiLineString') {
    return { ...geometry, coordinates: geometry.coordinates.map(line => simplifyRing(line, tolerance)) };
  }
  return geometry;
}

function largestPolygonOnly(feature) {
  if (!feature.geometry || feature.geometry.type !== 'MultiPolygon') return feature;
  const coords = feature.geometry.coordinates;
  let best = coords[0];
  let bestArea = -1;
  for (const poly of coords) {
    const a = ringArea(poly[0] || []);
    if (a > bestArea) {
      best = poly;
      bestArea = a;
    }
  }
  return { ...feature, geometry: { type: 'Polygon', coordinates: best } };
}

function readFeatures(fileMeta, project) {
  const fullPath = path.join(ROOT, fileMeta.path);
  if (!fs.existsSync(fullPath)) return [];
  let fc;
  try {
    fc = readJson(fullPath);
  } catch (err) {
    if (err.code === 'GIT_LFS_POINTER') {
      console.warn('Skipping Git LFS pointer. Run git lfs pull to download real GeoJSON:', fileMeta.path);
      return [];
    }
    console.warn('Skipping invalid GeoJSON:', fileMeta.path, err.message);
    return [];
  }
  const layerText = `${fileMeta.layerName || ''} ${fileMeta.layerKey || ''} ${fileMeta.path || ''}`;
  const pipeSize = pipeSizeFromText(layerText);
  return (fc.features || []).filter(f => f && f.geometry).map((f, i) => ({
    type: 'Feature',
    geometry: f.geometry,
    properties: cleanProps(f.properties || {}, {
      _id: `${project.key}:${fileMeta.layerKey || path.basename(fileMeta.path)}:${i}`,
      _project: project.name,
      _project_key: project.key,
      _layer: fileMeta.layerKey || '',
      _layer_display: fileMeta.layerName || '',
      _source_path: fileMeta.path,
      ...(pipeSize ? { _pipe_size: pipeSize } : {})
    })
  }));
}

function featureCollection(features) {
  return { type: 'FeatureCollection', features };
}

function main() {
  mkdirp(OUT_DIR);
  mkdirp(DETAIL_DIR);
  mkdirp(TILE_DIR);
  mkdirp(TMP_DIR);

  const manifest = readJson(MANIFEST_PATH);
  const coreFull = [];
  const coreSimplified = [];
  const irrigation = [];
  const heavyDetail = [];

  for (const project of manifest.projects || []) {
    const detailFeatures = [];

    for (const file of project.files || []) {
      const feats = readFeatures(file, project);
      if (!feats.length) continue;

      if (isCoreBlockPolygon(project, file)) {
        for (const f of feats) {
          const full = largestPolygonOnly(f);
          coreFull.push(full);
          coreSimplified.push({
            ...full,
            geometry: simplifyGeometry(full.geometry, 0.000035)
          });
        }
        continue;
      }

      if (isIrrigation(project, file) && file.type === 'line') {
        for (const f of feats) {
          irrigation.push({
            ...f,
            geometry: simplifyGeometry(f.geometry, 0.000008)
          });
        }
        continue;
      }

      if (!isChemical(project, file)) {
        for (const f of feats) {
          const simplified = file.type === 'polygon' || file.type === 'line'
            ? { ...f, geometry: simplifyGeometry(f.geometry, 0.000008) }
            : f;
          detailFeatures.push(simplified);
          if (isTreeLike(file, project) || file.type === 'point') heavyDetail.push(simplified);
        }
      }
    }

    writeJson(path.join(DETAIL_DIR, `${project.key}.geojson`), featureCollection(detailFeatures));
  }

  writeJson(path.join(OUT_DIR, 'core-blocks.geojson'), featureCollection(coreSimplified));
  writeJson(path.join(OUT_DIR, 'core-blocks-full.geojson'), featureCollection(coreFull));
  writeJson(path.join(OUT_DIR, 'irrigation-lines.geojson'), featureCollection(irrigation));

  const heavyPath = path.join(TMP_DIR, 'heavy-detail.geojson');
  writeJson(heavyPath, featureCollection(heavyDetail));

  const summary = {
    generatedAt: new Date().toISOString(),
    sourceManifest: 'data/map/manifest.json',
    coreBlocks: coreSimplified.length,
    irrigationLines: irrigation.length,
    heavyDetailFeatures: heavyDetail.length,
    outputs: {
      coreBlocks: 'data/optimized/core-blocks.geojson',
      coreBlocksFull: 'data/optimized/core-blocks-full.geojson',
      irrigationLines: 'data/optimized/irrigation-lines.geojson',
      blockDetails: 'data/optimized/block-details/<projectKey>.geojson',
      pmtilesOptional: 'data/optimized/tiles/heavy-detail.pmtiles'
    }
  };
  fs.writeFileSync(path.join(OUT_DIR, 'build-summary.json'), JSON.stringify(summary, null, 2));

  const tippecanoe = commandExists('tippecanoe');
  if (tippecanoe) {
    const out = path.join(TILE_DIR, 'heavy-detail.pmtiles');
    const cmd = [
      'tippecanoe', '-zg', '--projection=EPSG:4326', '--force',
      '--drop-densest-as-needed', '--extend-zooms-if-still-dropping',
      '--generate-ids', '-o', shellQuote(out), shellQuote(heavyPath)
    ].join(' ');
    console.log('Creating PMTiles:', cmd);
    childProcess.execSync(cmd, { stdio: 'inherit' });
  } else {
    fs.writeFileSync(path.join(TILE_DIR, 'README_PMtiles.txt'), [
      'Optional PMTiles/vector tile step:',
      '',
      'Install tippecanoe, then run from ArcGis-main:',
      'tippecanoe -zg --force --drop-densest-as-needed --extend-zooms-if-still-dropping --generate-ids -o data/optimized/tiles/heavy-detail.pmtiles data/optimized/_tmp/heavy-detail.geojson',
      '',
      'The app is already optimized with simplified GeoJSON. PMTiles is the next major speed upgrade for very large tree/detail layers.'
    ].join('\n'));
  }

  console.log('Enterprise map data generated:', summary);
}

function commandExists(command) {
  try {
    childProcess.execSync(`command -v ${command}`, { stdio: 'ignore', shell: '/bin/bash' });
    return true;
  } catch (_) {
    return false;
  }
}

function shellQuote(s) {
  return `'${String(s).replace(/'/g, `'\\''`)}'`;
}

main();
