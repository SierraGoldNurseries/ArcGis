#!/usr/bin/env python3
"""
Build SGN web-map data from files in data/raw.

Exports usable GIS layers from:
- .ppkx
- .mpkx
- .zip
- .gdb folders
- .geojson / .json / .kml / .shp

This version removes:
- Trees / tree detail / spacing layers
- Solar Panels 02
- Solar Panels 03
- Water Treatment
- Office
- Lab

It also renames "Structures / Yards" to "Structures".
"""

from __future__ import annotations

import csv
import json
import math
import re
import shutil
import subprocess
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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

HARD_DROP_PATTERNS = [
    r"\bsolar\s*panels?\s*0?2\b",
    r"\bsolar\s*panels?\s*0?3\b",
    r"\bwater\s*treatment\b",
    r"\boffice\b",
    r"\blab\b",
]

PROJECT_RULES = [
    ("Chemical Locations", ["chemical"]),
    ("Irrigation", ["irrigation"]),
    ("Owl Boxes", ["owl"]),
    ("Prune North", ["prune_north", "prune north", "prunenorth"]),
    ("Prune South", ["prune_south", "prune south", "prunesouth"]),
    ("Block #23", ["block_23", "block_#23", "block #23", "block23", "#23"]),
    ("Block #25", ["block_25", "block_#25", "block #25", "block25", "#25"]),
]

PROJECT_CONFIG = {
    "Structures": {
        "labels": True,
        "visible": True,
        "opacity": 0.26,
        "point_mode": "normal",
    },
    "Chemical Locations": {
        "labels": True,
        "visible": True,
        "opacity": 0.30,
        "point_mode": "normal",
    },
    "Irrigation": {
        "labels": False,
        "visible": True,
        "opacity": 0.35,
        "point_mode": "normal",
    },
    "Owl Boxes": {
        "labels": True,
        "visible": True,
        "opacity": 0.28,
        "point_mode": "normal",
    },
    "Prune North": {
        "labels": True,
        "visible": True,
        "opacity": 0.20,
        "point_mode": "normal",
    },
    "Prune South": {
        "labels": True,
        "visible": True,
        "opacity": 0.20,
        "point_mode": "normal",
    },
    "Block #23": {
        "labels": True,
        "visible": True,
        "opacity": 0.16,
        "point_mode": "normal",
    },
    "Block #25": {
        "labels": True,
        "visible": True,
        "opacity": 0.16,
        "point_mode": "normal",
    },
}


@dataclass
class GdbInput:
    path: Path
    source_name: str


@dataclass
class VectorInput:
    path: Path
    source_name: str


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(map(str, cmd)))
    p = subprocess.run(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if check and p.returncode:
        print(p.stdout)
        raise SystemExit(p.returncode)
    return p


def safe(name: Any) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", str(name)).strip("_") or "layer"


def norm(text: str) -> str:
    return re.sub(r"[^a-z0-9#]+", " ", text.lower()).strip()


def all_property_text(props: dict[str, Any]) -> str:
    vals = []

    for k, v in props.items():
        if isinstance(v, (str, int, float)):
            vals.append(str(k))
            vals.append(str(v))

    return norm(" ".join(vals))


def should_drop_feature(feat: dict[str, Any]) -> tuple[bool, str]:
    props = feat.get("properties") or {}
    blob = all_property_text(props)

    for pattern in HARD_DROP_PATTERNS:
        if re.search(pattern, blob, flags=re.I):
            return True, pattern

    return False, ""


def is_lfs_pointer(path: Path) -> bool:
    if not path.is_file():
        return False

    if path.stat().st_size > 2048:
        return False

    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False

    return "git-lfs.github.com/spec" in text


def project_from_source(source: str, layer: str = "") -> str:
    blob = norm(f"{source} {layer}")

    for project, tokens in PROJECT_RULES:
        for token in tokens:
            if norm(token) in blob:
                return project

    return "Structures"


def should_skip_layer(source_name: str, container_name: str, layer: str) -> tuple[bool, str]:
    blob = norm(f"{source_name} {container_name} {layer}")
    layer_clean = safe(layer).lower()

    if layer_clean in SYSTEM_LAYER_NAMES:
        return True, "system layer"

    skip_tokens = [
        "tree",
        "trees",
        "tree_backup",
        "trees_backup",
        "prune_tree",
        "mother_tree",
        "mother_trees",
        "individual_tree",
        "individual_trees",
        "tree_point",
        "tree_points",
        "spaces",
        "space",
        "spacing",
        "pointstoline",
        "points_to_line",
        "points2line",
    ]

    for token in skip_tokens:
        if token in blob:
            return True, f"skipped detail layer: {token}"

    return False, ""


def clean_label_text(value: Any) -> str:
    t = str(value or "")

    t = re.sub(r"\bStructures\s*/\s*Yards\b", "", t, flags=re.I)
    t = re.sub(r"\bStructures\b", "", t, flags=re.I)
    t = re.sub(r"\bYards\b", "", t, flags=re.I)
    t = re.sub(r"\bAcres?\b", "", t, flags=re.I)
    t = re.sub(r"\bAures\b", "", t, flags=re.I)
    t = re.sub(r"\s+", " ", t).strip()

    return t


def apply_project_properties(feat: dict[str, Any], source: str, layer: str) -> None:
    props = feat.setdefault("properties", {})
    project = project_from_source(source, layer)
    cfg = PROJECT_CONFIG.get(project, PROJECT_CONFIG["Structures"])

    props["_project"] = project
    props["_source"] = source
    props["_layer"] = clean_label_text(layer)
    props["_label_default"] = cfg["labels"]
    props["_default_visible"] = cfg["visible"]
    props["_display_opacity"] = cfg["opacity"]
    props["_point_mode"] = cfg["point_mode"]

    for key in ["Name", "name", "Label", "label", "Layer", "layer"]:
        if key in props and isinstance(props[key], str):
            props[key] = clean_label_text(props[key])

    object_id = (
        props.get("OBJECTID")
        or props.get("ObjectID")
        or props.get("FID")
        or props.get("Id")
        or props.get("id")
        or len(str(props))
    )

    props["_feature_key"] = safe(f"{project}_{layer}_{object_id}")


def reset_dirs() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    RAW.mkdir(parents=True, exist_ok=True)

    if TMP.exists():
        shutil.rmtree(TMP)
    TMP.mkdir(parents=True, exist_ok=True)

    if LAYERS_DIR.exists():
        shutil.rmtree(LAYERS_DIR)
    LAYERS_DIR.mkdir(parents=True, exist_ok=True)


def extract_archive(path: Path, dest: Path) -> bool:
    dest.mkdir(parents=True, exist_ok=True)

    if path.suffix.lower() == ".zip":
        try:
            with zipfile.ZipFile(path) as z:
                z.extractall(dest)
            return True
        except Exception as exc:
            print(f"Python zip extraction failed for {path.name}: {exc}")
            print("Trying 7z instead...")

    p = run(["7z", "x", "-y", f"-o{dest}", str(path)], check=False)

    if p.returncode:
        print(p.stdout)
        print(f"Warning: could not extract {path.name}; skipping.")
        return False

    return True


def dedupe_items(items: list[Any]) -> list[Any]:
    seen: set[str] = set()
    result: list[Any] = []

    for item in items:
        key = str(item.path.resolve())
        if key in seen:
            continue

        seen.add(key)
        result.append(item)

    return result


def discover_inputs() -> tuple[list[GdbInput], list[VectorInput]]:
    raw_items = [p for p in sorted(RAW.iterdir()) if p.name != ".gitkeep"]

    if not raw_items:
        raise SystemExit(
            "No input files found. Put .ppkx/.mpkx/.gdb.zip/.shp.zip/.geojson/.kml files in data/raw/."
        )

    gdbs: list[GdbInput] = []
    vectors: list[VectorInput] = []

    for item in raw_items:
        lower_name = item.name.lower()

        if is_lfs_pointer(item):
            print(f"Warning: skipping Git LFS pointer file, not a real package: {item.name}")
            continue

        skip_raw, reason = should_skip_layer(item.name, item.name, item.stem)
        if skip_raw:
            print(f"Skipping raw item {item.name}: {reason}")
            continue

        if item.is_dir() and lower_name.endswith(".gdb"):
            gdbs.append(GdbInput(path=item, source_name=item.name))
            continue

        if item.is_file() and item.suffix.lower() in VECTOR_EXTS:
            vectors.append(VectorInput(path=item, source_name=item.name))
            continue

        if item.is_file() and (item.suffix.lower() in PACKAGE_EXTS or item.suffix.lower() == ".zip"):
            dest = TMP / safe(item.stem)
            print(f"Extracting {item.name}...")

            if not extract_archive(item, dest):
                continue

            for gdb in sorted(dest.rglob("*.gdb")):
                skip_gdb, gdb_reason = should_skip_layer(item.name, gdb.name, gdb.stem)
                if skip_gdb:
                    print(f"Skipping geodatabase {gdb.name}: {gdb_reason}")
                    continue
                gdbs.append(GdbInput(path=gdb, source_name=item.name))

            for vec in sorted(dest.rglob("*")):
                if vec.is_file() and vec.suffix.lower() in VECTOR_EXTS:
                    if ".gdb" in [part.lower() for part in vec.parts]:
                        continue

                    skip_vec, vec_reason = should_skip_layer(item.name, vec.name, vec.stem)
                    if skip_vec:
                        print(f"Skipping vector {vec.name}: {vec_reason}")
                        continue

                    vectors.append(VectorInput(path=vec, source_name=item.name))

            continue

        print(f"Skipping unsupported raw item: {item.name}")

    gdbs = dedupe_items(gdbs)
    vectors = dedupe_items(vectors)

    print(f"Found {len(gdbs)} geodatabase(s) and {len(vectors)} standalone vector file(s).")

    if not gdbs and not vectors:
        raise SystemExit(
            "No usable GIS data found. Make sure data/raw contains real .ppkx/.mpkx/.zip/.gdb/vector files."
        )

    return gdbs, vectors


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

    clean_layers: list[str] = []
    seen: set[str] = set()

    for layer in layers:
        if layer not in seen:
            seen.add(layer)
            clean_layers.append(layer)

    return clean_layers


def layers_for_gdb(gdb_input: GdbInput) -> list[str]:
    gdb = gdb_input.path

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

    kept: list[str] = []
    skipped: list[str] = []

    for layer in layers:
        skip, reason = should_skip_layer(gdb_input.source_name, gdb.name, layer)

        if skip:
            skipped.append(f"{layer} ({reason})")
        else:
            kept.append(layer)

    print(f"Reading geodatabase from {gdb_input.source_name}: {gdb.name}")
    print(f"Found {len(layers)} layer(s).")

    if kept:
        print("Keeping layer(s):")
        for layer in kept:
            print(f"  - {layer}")

    if skipped:
        print("Skipping layer(s):")
        for layer in skipped:
            print(f"  - {layer}")

    return kept


def load_geojson(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"Warning: could not read GeoJSON {path}: {exc}")
        return None


def save_geojson(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")


def export_gdb_layer(gdb_input: GdbInput, layer: str, ordinal: int) -> tuple[Path | None, dict[str, Any]]:
    gdb = gdb_input.path
    source_name = gdb_input.source_name
    out = LAYERS_DIR / f"{ordinal:03d}_{safe(Path(source_name).stem)}_{safe(layer)}.geojson"

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

    if p.returncode:
        print(p.stdout)
        return None, {
            "source": source_name,
            "container": gdb.name,
            "layer": layer,
            "file": "",
            "features": 0,
            "geometry": "",
            "status": "export_failed",
        }

    if not out.exists() or out.stat().st_size <= 80:
        return None, {
            "source": source_name,
            "container": gdb.name,
            "layer": layer,
            "file": "",
            "features": 0,
            "geometry": "",
            "status": "empty_export",
        }

    data = load_geojson(out)

    if not data:
        return None, {
            "source": source_name,
            "container": gdb.name,
            "layer": layer,
            "file": out.name,
            "features": 0,
            "geometry": "",
            "status": "json_error",
        }

    features = data.get("features", []) or []
    cleaned_features: list[dict[str, Any]] = []
    dropped = 0

    for feat in features:
        geom = feat.get("geometry")

        if not geom:
            continue

        drop, reason = should_drop_feature(feat)
        if drop:
            dropped += 1
            continue

        apply_project_properties(feat, source_name, layer)
        cleaned_features.append(feat)

    data["features"] = cleaned_features
    save_geojson(out, data)

    geom_type = next(
        (f.get("geometry", {}).get("type", "") for f in cleaned_features if f.get("geometry")),
        "",
    )

    status = "ok"
    if dropped:
        status = f"ok_dropped_{dropped}_hidden_features"

    return out, {
        "source": source_name,
        "container": gdb.name,
        "layer": clean_label_text(layer),
        "file": out.name,
        "features": len(cleaned_features),
        "geometry": geom_type,
        "status": status,
    }


def export_vector_file(vector_input: VectorInput, ordinal: int) -> tuple[Path | None, dict[str, Any]]:
    src = vector_input.path
    source_name = vector_input.source_name
    layer = src.stem

    skip, reason = should_skip_layer(source_name, src.name, layer)

    if skip:
        return None, {
            "source": source_name,
            "container": src.name,
            "layer": layer,
            "file": "",
            "features": 0,
            "geometry": "",
            "status": f"skipped: {reason}",
        }

    out = LAYERS_DIR / f"{ordinal:03d}_{safe(Path(source_name).stem)}_{safe(layer)}.geojson"

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

    if p.returncode:
        print(p.stdout)
        return None, {
            "source": source_name,
            "container": src.name,
            "layer": layer,
            "file": "",
            "features": 0,
            "geometry": "",
            "status": "export_failed",
        }

    data = load_geojson(out)

    if not data:
        return None, {
            "source": source_name,
            "container": src.name,
            "layer": layer,
            "file": out.name,
            "features": 0,
            "geometry": "",
            "status": "json_error",
        }

    features = data.get("features", []) or []
    cleaned_features: list[dict[str, Any]] = []
    dropped = 0

    for feat in features:
        geom = feat.get("geometry")

        if not geom:
            continue

        drop, reason = should_drop_feature(feat)
        if drop:
            dropped += 1
            continue

        apply_project_properties(feat, source_name, layer)
        cleaned_features.append(feat)

    data["features"] = cleaned_features
    save_geojson(out, data)

    geom_type = next(
        (f.get("geometry", {}).get("type", "") for f in cleaned_features if f.get("geometry")),
        "",
    )

    status = "ok"
    if dropped:
        status = f"ok_dropped_{dropped}_hidden_features"

    return out, {
        "source": source_name,
        "container": src.name,
        "layer": clean_label_text(layer),
        "file": out.name,
        "features": len(cleaned_features),
        "geometry": geom_type,
        "status": status,
    }


def geometry_points(geom: dict[str, Any]) -> list[list[float]]:
    coords = geom.get("coordinates")
    pts: list[list[float]] = []

    def walk(x: Any) -> None:
        if (
            isinstance(x, (list, tuple))
            and len(x) >= 2
            and isinstance(x[0], (int, float))
            and isinstance(x[1], (int, float))
        ):
            pts.append([float(x[0]), float(x[1])])
            return

        if isinstance(x, (list, tuple)):
            for y in x:
                walk(y)

    walk(coords)
    return pts


def representative_point(geom: dict[str, Any]) -> list[float] | None:
    typ = geom.get("type")

    if typ == "Point":
        c = geom.get("coordinates")
        if isinstance(c, list) and len(c) >= 2:
            return [float(c[0]), float(c[1])]
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
        area = 0.0
        cx = 0.0
        cy = 0.0

        for i in range(len(ring) - 1):
            x1, y1 = float(ring[i][0]), float(ring[i][1])
            x2, y2 = float(ring[i + 1][0]), float(ring[i + 1][1])
            cross = x1 * y2 - x2 * y1
            area += cross
            cx += (x1 + x2) * cross
            cy += (y1 + y2) * cross

        if abs(area) > 1e-12:
            area *= 0.5
            return [cx / (6 * area), cy / (6 * area)]

    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]

    return [(min(xs) + max(xs)) / 2, (min(ys) + max(ys)) / 2]


def is_point_geometry(geom_type: str) -> bool:
    return geom_type in {"Point", "MultiPoint"}


def is_shape_geometry(geom_type: str) -> bool:
    return geom_type in {
        "LineString",
        "MultiLineString",
        "Polygon",
        "MultiPolygon",
        "GeometryCollection",
    }


def combine_and_split_geojson(files: list[Path]) -> tuple[dict[str, Any], dict[str, Any]]:
    shape_features: list[dict[str, Any]] = []
    point_features: list[dict[str, Any]] = []

    for file_path in files:
        data = load_geojson(file_path)

        if not data:
            continue

        for feat in data.get("features", []) or []:
            drop, reason = should_drop_feature(feat)
            if drop:
                continue

            geom = feat.get("geometry") or {}
            geom_type = geom.get("type", "")

            if is_point_geometry(geom_type):
                props = feat.setdefault("properties", {})
                props.setdefault("_export_file", file_path.name)
                props.setdefault("_generated_label_point", False)
                point_features.append(feat)
                continue

            if is_shape_geometry(geom_type):
                props = feat.setdefault("properties", {})
                props.setdefault("_export_file", file_path.name)
                shape_features.append(feat)

                label_point = representative_point(geom)

                if label_point and all(math.isfinite(x) for x in label_point):
                    label_props = dict(props)
                    label_props["_generated_label_point"] = True

                    point_features.append(
                        {
                            "type": "Feature",
                            "properties": label_props,
                            "geometry": {
                                "type": "Point",
                                "coordinates": label_point[:2],
                            },
                        }
                    )

    shapes = {
        "type": "FeatureCollection",
        "features": shape_features,
    }

    points = {
        "type": "FeatureCollection",
        "features": point_features,
    }

    return shapes, points


def write_outputs(exported_files: list[Path], inventory: list[dict[str, Any]]) -> None:
    shapes, points = combine_and_split_geojson(exported_files)

    save_geojson(OUT / "combined_polygons.geojson", shapes)
    save_geojson(OUT / "combined_points.geojson", points)

    save_geojson(OUT / "structures_polygons.geojson", shapes)
    save_geojson(OUT / "structures_points.geojson", points)

    project_counts: dict[str, int] = {}

    for feat in (shapes.get("features", []) or []) + (points.get("features", []) or []):
        project = feat.get("properties", {}).get("_project", "Structures")
        if project == "Structures / Yards":
            project = "Structures"
        project_counts[project] = project_counts.get(project, 0) + 1

    (OUT / "map_config.json").write_text(
        json.dumps({"projects": PROJECT_CONFIG}, indent=2),
        encoding="utf-8",
    )

    (OUT / "project_manifest.json").write_text(
        json.dumps(
            {
                "projects": project_counts,
                "shape_features": len(shapes.get("features", []) or []),
                "point_features": len(points.get("features", []) or []),
                "total_features": len(shapes.get("features", []) or []) + len(points.get("features", []) or []),
                "note": "Tree/detail layers and hard-hidden facility items removed. Structures / Yards renamed to Structures.",
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    with (OUT / "layer_inventory.csv").open("w", newline="", encoding="utf-8") as f:
        fields = [
            "source",
            "container",
            "layer",
            "file",
            "features",
            "geometry",
            "status",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(inventory)

    print("")
    print("DONE")
    print(f"Shape features written: {len(shapes.get('features', []) or [])}")
    print(f"Point/label features written: {len(points.get('features', []) or [])}")
    print(f"Total features written: {len(shapes.get('features', []) or []) + len(points.get('features', []) or [])}")


def main() -> None:
    reset_dirs()

    gdbs, vectors = discover_inputs()

    exported_files: list[Path] = []
    inventory: list[dict[str, Any]] = []
    ordinal = 1

    for gdb_input in gdbs:
        layers = layers_for_gdb(gdb_input)

        if not layers:
            inventory.append(
                {
                    "source": gdb_input.source_name,
                    "container": gdb_input.path.name,
                    "layer": "",
                    "file": "",
                    "features": 0,
                    "geometry": "",
                    "status": "no_readable_layers",
                }
            )
            continue

        for layer in layers:
            out, row = export_gdb_layer(gdb_input, layer, ordinal)
            ordinal += 1
            inventory.append(row)

            if out:
                exported_files.append(out)

    for vector_input in vectors:
        out, row = export_vector_file(vector_input, ordinal)
        ordinal += 1
        inventory.append(row)

        if out:
            exported_files.append(out)

    if not exported_files:
        raise SystemExit(
            "No layers were exported. Check that data/raw contains real .ppkx/.mpkx files, not Git LFS pointer files."
        )

    write_outputs(exported_files, inventory)


if __name__ == "__main__":
    main()
