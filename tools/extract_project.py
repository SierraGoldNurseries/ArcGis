#!/usr/bin/env python3
"""Build SGN web-map data from ArcGIS/GIS project files in data/raw/.

This version intentionally exports polygon/block boundary layers only.
It skips individual tree points / point-to-line tree rows so the web map does
not plot every tree.
"""
from __future__ import annotations

import csv
import json
import math
import re
import shutil
import subprocess
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data"
TMP = ROOT / "_arcgis_extract_tmp"
LAYERS_DIR = OUT / "layers"

VECTOR_EXTS = {".geojson", ".json", ".kml", ".shp"}
PACKAGE_EXTS = {".ppkx", ".mpkx"}

SYSTEM_LAYER_NAMES = {
    "gdb_items",
    "gdb_itemtypes",
    "gdb_itemrelationships",
    "gdb_itemrelationshiptypes",
    "gdb_spatialrefs",
    "gdb_tableslastmodified",
    "gdb_replicalog",
}

# These layers create thousands of dots/pins. Skip them.
SKIP_LAYER_TOKENS = [
    "tree",
    "trees",
    "pointstoline",
    "points_to_line",
    "spacing",
    "exportfeatures1",
    "mother_trees",
    "mother_tree",
    "individual",
    "sample",
    "gps_point",
    "gps_points",
    "point",
    "points",
]

# Keep polygon/boundary/block layers even if their name contains prune/south/etc.
KEEP_LAYER_TOKENS = [
    "block",
    "boundary",
    "boundaries",
    "outline",
    "polygon",
    "polygons",
    "field",
    "fields",
    "ranch",
    "yard",
    "can_yard",
    "shadehouse",
    "high_tunnel",
    "tunnel",
    "parcel",
    "prune_south",
    "prune_north",
]

PROJECT_RULES = [
    ("Chemical Locations", ["chemical"]),
    ("Irrigation", ["irrigation"]),
    ("Owl Boxes", ["owl"]),
    ("Prune North", ["prune_north", "prune north", "prunenorth"]),
    ("Prune South", ["prune_south", "prune south", "prunesouth"]),
    ("Block #23", ["block_23", "block_#23", "block #23", "block23", "23"]),
    ("Block #25", ["block_25", "block_#25", "block #25", "block25", "25"]),
]

PROJECT_CONFIG = {
    "Structures / Yards": {"labels": True, "visible": True, "opacity": 0.24, "point_mode": "normal"},
    "Chemical Locations": {"labels": True, "visible": True, "opacity": 0.30, "point_mode": "normal"},
    "Irrigation": {"labels": False, "visible": True, "opacity": 0.26, "point_mode": "normal"},
    "Owl Boxes": {"labels": True, "visible": True, "opacity": 0.28, "point_mode": "normal"},
    "Prune North": {"labels": True, "visible": True, "opacity": 0.20, "point_mode": "normal"},
    "Prune South": {"labels": True, "visible": True, "opacity": 0.20, "point_mode": "normal"},
    "Block #23": {"labels": True, "visible": True, "opacity": 0.12, "point_mode": "normal"},
    "Block #25": {"labels": True, "visible": True, "opacity": 0.12, "point_mode": "normal"},
}


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print("+", " ".join(map(str, cmd)))
    p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if check and p.returncode:
        print(p.stdout)
        raise SystemExit(p.returncode)
    return p


def safe(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", str(name)).strip("_") or "layer"


def layer_blob(source: str, layer: str = "") -> str:
    return f"{source} {layer}".lower().replace("-", "_").replace(" ", "_")


def should_skip_layer(source: str, layer: str) -> bool:
    blob = layer_blob(source, layer)

    # If it clearly says block/boundary/outline/etc., keep it.
    if any(tok in blob for tok in KEEP_LAYER_TOKENS):
        # But still skip obvious tree point layers.
        if any(tok in blob for tok in ["pointstoline", "points_to_line", "tree_spacing", "spacing_points"]):
            return True
        return False

    # Skip individual points / trees.
    if any(tok in blob for tok in SKIP_LAYER_TOKENS):
        return True

    return False


def is_lfs_pointer(path: Path) -> bool:
    if not path.is_file() or path.stat().st_size > 1024:
        return False
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False
    return "git-lfs.github.com/spec" in text


def project_from_source(source: str, layer: str = "") -> str:
    blob = layer_blob(source, layer)
    for project, tokens in PROJECT_RULES:
        if any(t.replace(" ", "_") in blob for t in tokens):
            return project
    return "Structures / Yards"


def apply_project_properties(feat: dict, source: str, layer: str) -> None:
    props = feat.setdefault("properties", {})
    project = project_from_source(source, layer)
    cfg = PROJECT_CONFIG.get(project, PROJECT_CONFIG["Structures / Yards"])

    props.setdefault("_project", project)
    props.setdefault("_label_default", cfg["labels"])
    props.setdefault("_default_visible", cfg["visible"])
    props.setdefault("_display_opacity", cfg["opacity"])
    props.setdefault("_point_mode", cfg["point_mode"])
    props.setdefault("_layer", layer)
    props.setdefault("_source", source)
    props.setdefault(
        "_feature_key",
        safe(f"{project}_{layer}_{props.get('OBJECTID') or props.get('FID') or len(str(props))}"),
    )


def reset_dirs() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    RAW.mkdir(parents=True, exist_ok=True)

    if TMP.exists():
        shutil.rmtree(TMP)
    TMP.mkdir(parents=True, exist_ok=True)

    if LAYERS_DIR.exists():
        shutil.rmtree(LAYERS_DIR)
    LAYERS_DIR.mkdir(parents=True, exist_ok=True)


def extract_archive(path: Path, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)

    if path.suffix.lower() == ".zip":
        try:
            with zipfile.ZipFile(path) as z:
                z.extractall(dest)
            return
        except Exception as exc:
            print(f"Python zip extraction failed for {path.name}: {exc}; trying 7z")

    run(["7z", "x", "-y", f"-o{dest}", str(path)])


def discover_inputs() -> tuple[list[Path], list[Path]]:
    raw_items = [p for p in sorted(RAW.iterdir()) if p.name != ".gitkeep"]

    if not raw_items:
        raise SystemExit("No input files found. Put .ppkx/.mpkx/.gdb.zip/.shp.zip/.geojson/.kml files in data/raw/.")

    gdbs: list[Path] = []
    vectors: list[Path] = []

    for item in raw_items:
        lower_name = item.name.lower()

        if is_lfs_pointer(item):
            print(f"Warning: skipping Git LFS pointer file because the real file is not downloaded: {item}")
            continue

        if item.is_dir() and lower_name.endswith(".gdb"):
            gdbs.append(item)
            continue

        if item.is_file() and item.suffix.lower() in VECTOR_EXTS:
            vectors.append(item)
            continue

        if item.is_file() and (item.suffix.lower() in PACKAGE_EXTS or item.suffix.lower() == ".zip"):
            dest = TMP / safe(item.stem)
            print(f"Extracting {item.name}...")

            try:
                extract_archive(item, dest)
            except SystemExit:
                print(f"Warning: could not extract {item.name}; skipping it.")
                continue

            gdbs.extend(dest.rglob("*.gdb"))
            vectors.extend([p for p in dest.rglob("*") if p.is_file() and p.suffix.lower() in VECTOR_EXTS])
            continue

        print(f"Skipping unsupported raw item: {item.name}")

    vectors = [v for v in vectors if ".gdb" not in [part.lower() for part in v.parts]]

    def dedupe(paths: list[Path]) -> list[Path]:
        seen: set[str] = set()
        result: list[Path] = []
        for p in paths:
            k = str(p.resolve())
            if k not in seen:
                seen.add(k)
                result.append(p)
        return result

    return dedupe(gdbs), dedupe(vectors)


def parse_ogr_layer_list(output: str) -> list[str]:
    layers: list[str] = []

    for line in output.splitlines():
        m = re.match(r"^\s*\d+\s*:\s+(.+?)(?:\s+\([^()]*\))?\s*$", line)
        if m:
            layer = m.group(1).strip()
            if layer and safe(layer).lower() not in SYSTEM_LAYER_NAMES:
                layers.append(layer)
            continue

        m = re.match(r"^\s*Layer name:\s+(.+?)\s*$", line, flags=re.I)
        if m:
            layer = m.group(1).strip()
            if layer and safe(layer).lower() not in SYSTEM_LAYER_NAMES:
                layers.append(layer)

    seen: set[str] = set()
    clean: list[str] = []

    for layer in layers:
        if layer not in seen:
            seen.add(layer)
            clean.append(layer)

    return clean


def layers_for_gdb(gdb: Path) -> list[str]:
    p = run(["ogrinfo", "-ro", str(gdb)], check=False)

    if p.returncode:
        print(p.stdout)
        return []

    layers = parse_ogr_layer_list(p.stdout)

    if not layers:
        p2 = run(["ogrinfo", "-ro", "-al", "-so", str(gdb)], check=False)
        if p2.returncode:
            print(p2.stdout)
            return []
        layers = parse_ogr_layer_list(p2.stdout)

    kept = []
    skipped = []

    for layer in layers:
        if should_skip_layer(gdb.name, layer):
            skipped.append(layer)
        else:
            kept.append(layer)

    print(f"Found {len(layers)} layer(s) in {gdb.name}.")
    print(f"Keeping boundary/block layer(s): {', '.join(kept) if kept else 'none'}")
    if skipped:
        print(f"Skipping tree/point layer(s): {', '.join(skipped)}")

    return kept


def load_geojson(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"Warning: could not read exported GeoJSON {path}: {exc}")
        return None


def keep_boundary_features_only(data: dict, source: str, layer: str) -> dict:
    kept = []

    for feat in data.get("features", []) or []:
        geom = feat.get("geometry") or {}
        geom_type = geom.get("type", "")

        # Do not keep individual point/line features.
        # Only outside block/boundary polygons should remain.
        if geom_type not in {"Polygon", "MultiPolygon"}:
            continue

        apply_project_properties(feat, source, layer)
        kept.append(feat)

    return {"type": "FeatureCollection", "features": kept}


def export_gdb_layer(gdb: Path, layer: str, ordinal: int) -> tuple[Path | None, dict]:
    out = LAYERS_DIR / f"{ordinal:03d}_{safe(gdb.stem)}_{safe(layer)}.geojson"

    cmd = [
        "ogr2ogr",
        "-f",
        "GeoJSON",
        "-t_srs",
        "EPSG:4326",
        str(out),
        str(gdb),
        layer,
        "-nln",
        safe(layer),
        "-lco",
        "RFC7946=YES",
        "-skipfailures",
    ]

    p = run(cmd, check=False)

    if p.returncode != 0:
        print(p.stdout)
        return None, {
            "source": gdb.name,
            "layer": layer,
            "file": "",
            "features": 0,
            "geometry": "",
            "status": "export_failed",
        }

    if not out.exists() or out.stat().st_size <= 80:
        return None, {
            "source": gdb.name,
            "layer": layer,
            "file": "",
            "features": 0,
            "geometry": "",
            "status": "empty_export",
        }

    data = load_geojson(out)
    if not data:
        return None, {
            "source": gdb.name,
            "layer": layer,
            "file": out.name,
            "features": 0,
            "geometry": "",
            "status": "json_error",
        }

    filtered = keep_boundary_features_only(data, gdb.name, layer)
    features = filtered.get("features", []) or []

    if not features:
        return None, {
            "source": gdb.name,
            "layer": layer,
            "file": "",
            "features": 0,
            "geometry": "",
            "status": "no_polygon_boundaries_after_filter",
        }

    out.write_text(json.dumps(filtered, separators=(",", ":")), encoding="utf-8")

    geom = next((f.get("geometry", {}).get("type", "") for f in features if f.get("geometry")), "")

    return out, {
        "source": gdb.name,
        "layer": layer,
        "file": out.name,
        "features": len(features),
        "geometry": geom,
        "status": "ok",
    }


def export_vector_file(src: Path, ordinal: int) -> tuple[Path | None, dict]:
    layer = src.stem

    if should_skip_layer(src.name, layer):
        return None, {
            "source": src.name,
            "layer": layer,
            "file": "",
            "features": 0,
            "geometry": "",
            "status": "skipped_tree_or_point_layer",
        }

    out = LAYERS_DIR / f"{ordinal:03d}_{safe(layer)}.geojson"

    cmd = [
        "ogr2ogr",
        "-f",
        "GeoJSON",
        "-t_srs",
        "EPSG:4326",
        str(out),
        str(src),
        "-lco",
        "RFC7946=YES",
        "-skipfailures",
    ]

    p = run(cmd, check=False)

    if p.returncode != 0:
        print(p.stdout)
        return None, {
            "source": src.name,
            "layer": layer,
            "file": "",
            "features": 0,
            "geometry": "",
            "status": "export_failed",
        }

    data = load_geojson(out)
    if not data:
        return None, {
            "source": src.name,
            "layer": layer,
            "file": out.name,
            "features": 0,
            "geometry": "",
            "status": "json_error",
        }

    filtered = keep_boundary_features_only(data, src.name, layer)
    features = filtered.get("features", []) or []

    if not features:
        return None, {
            "source": src.name,
            "layer": layer,
            "file": "",
            "features": 0,
            "geometry": "",
            "status": "no_polygon_boundaries_after_filter",
        }

    out.write_text(json.dumps(filtered, separators=(",", ":")), encoding="utf-8")

    geom = next((f.get("geometry", {}).get("type", "") for f in features if f.get("geometry")), "")

    return out, {
        "source": src.name,
        "layer": layer,
        "file": out.name,
        "features": len(features),
        "geometry": geom,
        "status": "ok",
    }


def combine_geojson(files: list[Path]) -> dict:
    features: list[dict] = []

    for f in files:
        data = load_geojson(f)

        if not data:
            continue

        layer_default = re.sub(r"^\d+_", "", f.stem)

        for feat in data.get("features", []) or []:
            geom_type = (feat.get("geometry") or {}).get("type", "")
            if geom_type not in {"Polygon", "MultiPolygon"}:
                continue

            feat.setdefault("properties", {})
            feat["properties"].setdefault("_layer", layer_default)
            feat["properties"].setdefault("_export_file", f.name)
            features.append(feat)

    return {"type": "FeatureCollection", "features": features}


def geometry_points(geom: dict) -> list[list[float]]:
    coords = geom.get("coordinates")
    pts: list[list[float]] = []

    def walk(x) -> None:
        if isinstance(x, (list, tuple)) and len(x) >= 2 and all(isinstance(v, (int, float)) for v in x[:2]):
            pts.append([float(x[0]), float(x[1])])
        elif isinstance(x, (list, tuple)):
            for y in x:
                walk(y)

    walk(coords)
    return pts


def representative_point(geom: dict) -> list[float] | None:
    typ = geom.get("type")

    # Important: do NOT generate points from source Point features.
    # Points file should only contain one label/center point per polygon boundary.
    if typ not in {"Polygon", "MultiPolygon"}:
        return None

    pts = geometry_points(geom)

    if not pts:
        return None

    rings = None

    if typ == "Polygon":
        rings = geom.get("coordinates")
    elif typ == "MultiPolygon" and geom.get("coordinates"):
        rings = geom.get("coordinates")[0]

    if rings and rings[0] and len(rings[0]) >= 4:
        ring = rings[0]
        a = cx = cy = 0.0

        for i in range(len(ring) - 1):
            x1, y1 = float(ring[i][0]), float(ring[i][1])
            x2, y2 = float(ring[i + 1][0]), float(ring[i + 1][1])
            cross = x1 * y2 - x2 * y1
            a += cross
            cx += (x1 + x2) * cross
            cy += (y1 + y2) * cross

        if abs(a) > 1e-12:
            a *= 0.5
            return [cx / (6 * a), cy / (6 * a)]

    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]

    return [(min(xs) + max(xs)) / 2, (min(ys) + max(ys)) / 2]


def make_points(fc: dict) -> dict:
    pts: list[dict] = []

    for i, f in enumerate(fc.get("features", []) or []):
        geom = f.get("geometry") or {}
        p = representative_point(geom)

        if not p or not all(math.isfinite(x) for x in p):
            continue

        props = dict(f.get("properties") or {})
        props["generated_point"] = True
        props.setdefault("_point_id", i + 1)

        pts.append({
            "type": "Feature",
            "properties": props,
            "geometry": {
                "type": "Point",
                "coordinates": p[:2],
            },
        })

    return {"type": "FeatureCollection", "features": pts}


def write_outputs(feature_files: list[Path], inventory: list[dict]) -> None:
    combined = combine_geojson(feature_files)
    points = make_points(combined)

    (OUT / "combined_polygons.geojson").write_text(json.dumps(combined, separators=(",", ":")), encoding="utf-8")
    (OUT / "combined_points.geojson").write_text(json.dumps(points, separators=(",", ":")), encoding="utf-8")

    # Backward-compatible filenames used by older map pages.
    (OUT / "structures_polygons.geojson").write_text(json.dumps(combined, separators=(",", ":")), encoding="utf-8")
    (OUT / "structures_points.geojson").write_text(json.dumps(points, separators=(",", ":")), encoding="utf-8")

    project_counts: dict[str, int] = {}

    for feat in combined.get("features", []) or []:
        project = feat.get("properties", {}).get("_project", "Structures / Yards")
        project_counts[project] = project_counts.get(project, 0) + 1

    (OUT / "map_config.json").write_text(json.dumps({"projects": PROJECT_CONFIG}, indent=2), encoding="utf-8")

    (OUT / "project_manifest.json").write_text(
        json.dumps(
            {
                "projects": project_counts,
                "total_boundary_features": len(combined.get("features", []) or []),
                "total_label_points": len(points.get("features", []) or []),
                "note": "Individual tree/point layers are intentionally skipped. Only polygon/block boundaries are exported.",
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    with (OUT / "layer_inventory.csv").open("w", newline="", encoding="utf-8") as f:
        fields = ["source", "layer", "file", "features", "geometry", "status"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(inventory)

    print(
        f"Wrote {len(combined.get('features', []) or [])} boundary polygon feature(s) "
        f"and {len(points.get('features', []) or [])} label/display point(s)."
    )


def main() -> None:
    reset_dirs()
    gdbs, vectors = discover_inputs()

    inventory: list[dict] = []
    exported: list[Path] = []
    ordinal = 1

    for gdb in gdbs:
        print(f"Reading geodatabase: {gdb}")
        layers = layers_for_gdb(gdb)

        if not layers:
            inventory.append({
                "source": gdb.name,
                "layer": "",
                "file": "",
                "features": 0,
                "geometry": "",
                "status": "no_boundary_layers_or_unreadable",
            })
            continue

        for layer in layers:
            out, row = export_gdb_layer(gdb, layer, ordinal)
            ordinal += 1
            inventory.append(row)

            if out:
                exported.append(out)

    for src in vectors:
        print(f"Reading vector file: {src}")
        out, row = export_vector_file(src, ordinal)
        ordinal += 1
        inventory.append(row)

        if out:
            exported.append(out)

    if not exported:
        raise SystemExit(
            "No boundary polygon layers were exported. The raw file may only contain tree/point layers, "
            "or the real ArcGIS package may not be available because Git LFS is over budget."
        )

    write_outputs(exported, inventory)


if __name__ == "__main__":
    main()
