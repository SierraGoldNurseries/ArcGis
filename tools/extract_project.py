#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from osgeo import ogr, osr
except Exception as exc:
    print("ERROR: GDAL/OGR is required:", exc)
    sys.exit(2)

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
MAP_DIR = ROOT / "data" / "map"
PROJECTS_DIR = MAP_DIR / "projects"

PROJECT_RULES = [
    ("chemical", "chemical_locations", "Chemical Locations", "#dc2626", True, True),
    ("irrigation", "irrigation", "Irrigation", "#2563eb", True, True),
    ("owl", "owl_boxes", "Owl Boxes", "#7c3aed", True, True),
    ("tree", "trees", "Trees", "#16a34a", True, True),
    ("prune_north", "prune_north", "Prune North", "#9333ea", True, True),
    ("prunenorth", "prune_north", "Prune North", "#9333ea", True, True),
    ("prune_south", "prune_south", "Prune South", "#c026d3", True, True),
    ("prunesouth", "prune_south", "Prune South", "#c026d3", True, True),
    ("railroad_south", "railroad_south", "Railroad South", "#0f766e", True, True),
    ("railroadsouth", "railroad_south", "Railroad South", "#0f766e", True, True),
    ("block_#23", "block_23", "Block #23", "#f59e0b", True, True),
    ("block_23", "block_23", "Block #23", "#f59e0b", True, True),
    ("block23", "block_23", "Block #23", "#f59e0b", True, True),
    ("block_#25", "block_25", "Block #25", "#ea580c", True, True),
    ("block_25", "block_25", "Block #25", "#ea580c", True, True),
    ("block25", "block_25", "Block #25", "#ea580c", True, True),
    ("structure", "structures", "Structures / Yards", "#0f766e", True, True),
    ("yard", "structures", "Structures / Yards", "#0f766e", True, True),
]

COLOR_PALETTE = [
    "#0f766e", "#0284c7", "#7c3aed", "#b45309", "#dc2626",
    "#0891b2", "#4f46e5", "#be185d", "#15803d", "#9333ea",
    "#64748b", "#f59e0b", "#ea580c", "#14b8a6", "#2563eb",
    "#16a34a", "#ca8a04", "#0ea5e9", "#7e22ce", "#c2410c",
    "#65a30d", "#0369a1", "#a16207", "#be123c", "#047857"
]

GENERIC_LABELS = {
    "polygon", "polygons", "point", "points", "line", "lines",
    "layer", "layers", "shape", "shapes", "feature", "features",
    "area", "areas", "block", "blocks", "multipolygon", "multipolygons"
}

PREFERRED_LABEL_FIELDS = [
    "Name", "NAME", "name",
    "Label", "LABEL", "label",
    "Title", "TITLE", "title",
    "Block", "BLOCK", "block",
    "BlockName", "BLOCKNAME", "blockname",
    "Block_Name", "block_name",
    "Orchard", "ORCHARD", "orchard",
    "Variety", "VARIETY", "variety",
    "Site", "SITE", "site",
    "AreaName", "AREANAME", "area_name",
    "Description", "DESCRIPTION", "description"
]

# Derived/helper layers that should not be in the web map.
SKIP_LAYER_TOKENS = [
    "pointstoline",
    "points_to_line",
    "buffer",
    "exporttable",
    "export_table",
    "mother_tree",
    "vacant_export",
    "occupied_export",
    "sort",
]

# For Trees, keep real spacing point exports and basic tree/block polygons.
# Skip duplicate XY table layers, buffers, points-to-line, and helper export tables.
TREE_KEEP_POINT_TOKENS = [
    "spaces_exportfeatures",
    "space_exportfeatures",
]
TREE_KEEP_POLYGON_TOKENS = [
    "polygon",
    "polygons",
    "orchard",
    "block",
]


def log(msg: str) -> None:
    print(f"[extract] {msg}", flush=True)


def slug(value: Any) -> str:
    s = Path(str(value)).stem
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_#]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s.lower() or "unknown"


def display(value: Any) -> str:
    return slug(value).replace("_", " ").title()


def project_meta(path: Path) -> Dict[str, Any]:
    key = slug(path.name)
    compact = key.replace("_", "").replace("#", "")
    for token, project_key, name, color, visible, labels in PROJECT_RULES:
        t1 = token.lower()
        t2 = t1.replace("_", "").replace("#", "")
        if t1 in key or t2 in compact:
            return {
                "key": project_key,
                "name": name,
                "color": color,
                "defaultVisible": visible,
                "defaultLabels": labels,
            }
    return {
        "key": "other",
        "name": "Other",
        "color": "#64748b",
        "defaultVisible": True,
        "defaultLabels": True,
    }


def color_for(key: str) -> str:
    h = 0
    for ch in str(key):
        h = ((h << 5) - h + ord(ch)) & 0xFFFFFFFF
    return COLOR_PALETTE[h % len(COLOR_PALETTE)]


def is_meaningful_label(value: Any) -> bool:
    if value is None:
        return False
    s = str(value).strip()
    if not s:
        return False
    low = s.lower().strip()
    if low in GENERIC_LABELS:
        return False
    if re.fullmatch(r"[\d\W_]+", low):
        return False
    return True


def best_label(props: Dict[str, Any], layer_name: str, project_name: str, fid: int) -> str:
    for field in PREFERRED_LABEL_FIELDS:
        if field in props and is_meaningful_label(props[field]):
            return str(props[field]).strip()

    for key, val in props.items():
        low = str(key).lower()
        if any(token in low for token in ["name", "label", "block", "site", "area", "orchard", "variety"]):
            if is_meaningful_label(val):
                return str(val).strip()

    clean_layer = slug(layer_name)
    if clean_layer in GENERIC_LABELS:
        if project_name.lower().startswith("block"):
            return f"{project_name} {fid + 1}"
        return ""

    return display(layer_name)


def group_key(label: str, layer_name: str, project_name: str) -> str:
    base = label or layer_name or project_name
    normalized = slug(base)
    layer_slug = slug(layer_name)

    patterns = [
        r"^(high_tunnels_\d+)(?:_[a-z])?$",
        r"^(can_yard_\d+)(?:_[a-z])?$",
        r"^(shade_house_\d+|shadehouse_\d+)(?:_[a-z])?$",
        r"^(cold_frame_\d+)$",
        r"^(block_#?\d+).*$",
        r"^(four_bays?_\d+)$",
    ]

    for pat in patterns:
        m = re.match(pat, layer_slug, flags=re.I)
        if m:
            return slug(m.group(1))

    return normalized


def extract_archive(src: Path, dst: Path) -> bool:
    log(f"Extracting {src.name}...")
    try:
        subprocess.run(
            ["7z", "x", "-y", f"-o{dst}", str(src)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        return True
    except Exception as exc:
        log(f"7z failed for {src.name}: {exc}. Trying zipfile...")

    try:
        with zipfile.ZipFile(src) as z:
            z.extractall(dst)
        return True
    except Exception as exc:
        log(f"WARNING: could not extract {src.name}: {exc}")
        return False


def find_gdbs(folder: Path) -> List[Path]:
    return sorted([p for p in folder.rglob("*.gdb") if p.is_dir()])


def spatial_transform(layer: ogr.Layer) -> Optional[osr.CoordinateTransformation]:
    src = layer.GetSpatialRef()
    if src is None:
        return None
    dst = osr.SpatialReference()
    dst.ImportFromEPSG(4326)
    if src.IsSame(dst):
        return None
    return osr.CoordinateTransformation(src, dst)


def geom_kind_from_type(flat_type: int) -> str:
    if flat_type in (ogr.wkbPolygon, ogr.wkbMultiPolygon):
        return "polygon"
    if flat_type in (ogr.wkbPoint, ogr.wkbMultiPoint):
        return "point"
    if flat_type in (ogr.wkbLineString, ogr.wkbMultiLineString):
        return "line"
    return "other"


def geom_kind_from_geom(geom: ogr.Geometry) -> str:
    if geom is None or geom.IsEmpty():
        return "other"
    return geom_kind_from_type(ogr.GT_Flatten(geom.GetGeometryType()))


def should_skip_layer(project_key: str, layer_name: str, kind: str) -> bool:
    low = slug(layer_name)

    if any(token in low for token in SKIP_LAYER_TOKENS):
        return True

    if project_key == "trees":
        if kind == "line":
            return True
        if kind == "point":
            return not any(token in low for token in TREE_KEEP_POINT_TOKENS)
        if kind == "polygon":
            return not any(token in low for token in TREE_KEEP_POLYGON_TOKENS)

    return False


def feature_to_geojson(feat: ogr.Feature, layer: ogr.Layer, transform: Optional[osr.CoordinateTransformation]) -> Optional[Dict[str, Any]]:
    geom = feat.GetGeometryRef()
    if geom is None or geom.IsEmpty():
        return None

    geom = geom.Clone()

    if transform is not None:
        try:
            geom.Transform(transform)
        except Exception:
            return None

    try:
        geom_json = json.loads(geom.ExportToJson())
    except Exception:
        return None

    props: Dict[str, Any] = {}
    defn = layer.GetLayerDefn()

    for i in range(defn.GetFieldCount()):
        try:
            name = defn.GetFieldDefn(i).GetName()
            value = feat.GetField(i)
            props[name] = value
        except Exception:
            pass

    return {
        "type": "Feature",
        "geometry": geom_json,
        "properties": props
    }


def add_meta(
    feature: Dict[str, Any],
    meta: Dict[str, Any],
    package_name: str,
    gdb_name: str,
    layer_name: str,
    fid: int,
    kind: str,
) -> Dict[str, Any]:
    props = feature.setdefault("properties", {})
    label = best_label(props, layer_name, meta["name"], fid)
    gkey = group_key(label, layer_name, meta["name"])
    gcolor = color_for(gkey)

    props["_project"] = meta["name"]
    props["_project_key"] = meta["key"]
    props["_project_color"] = meta["color"]
    props["_default_visible"] = meta["defaultVisible"]
    props["_default_labels"] = meta["defaultLabels"]
    props["_package"] = package_name
    props["_gdb"] = gdb_name
    props["_layer"] = layer_name
    props["_layer_key"] = slug(layer_name)
    props["_layer_display"] = "" if slug(layer_name) in GENERIC_LABELS else display(layer_name)
    props["_label"] = label
    props["_group_key"] = gkey
    props["_group_color"] = gcolor
    props["_fid"] = fid
    props["_geometry_kind"] = kind
    props["_feature_key"] = f"{meta['key']}::{slug(layer_name)}::{fid}"
    return feature


def geom_hash(feature: Dict[str, Any]) -> str:
    raw = json.dumps(feature.get("geometry"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def add_feature(
    store: Dict[Tuple[str, str, str], List[Dict[str, Any]]],
    seen: Dict[Tuple[str, str, str], set],
    project_key: str,
    layer_key: str,
    kind: str,
    feature: Dict[str, Any],
) -> None:
    key = (project_key, layer_key, kind)
    h = geom_hash(feature)
    if key not in seen:
        seen[key] = set()
    if h in seen[key]:
        return
    seen[key].add(h)
    store.setdefault(key, []).append(feature)


def read_gdb(
    gdb: Path,
    package_name: str,
    meta: Dict[str, Any],
    store: Dict[Tuple[str, str, str], List[Dict[str, Any]]],
    seen: Dict[Tuple[str, str, str], set],
    inventory: List[Dict[str, Any]],
    skipped: List[Dict[str, Any]],
) -> None:
    log(f"Reading geodatabase: {gdb}")

    try:
        ds = ogr.Open(str(gdb), 0)
    except Exception as exc:
        log(f"WARNING: skipping unreadable geodatabase {gdb}: {exc}")
        return

    if ds is None:
        log(f"WARNING: skipping unreadable geodatabase {gdb}")
        return

    if ds.GetLayerCount() <= 0:
        log(f"WARNING: skipping empty geodatabase {gdb}")
        return

    for i in range(ds.GetLayerCount()):
        layer = ds.GetLayerByIndex(i)
        if layer is None:
            continue

        layer_name = layer.GetName() or ""
        low = layer_name.lower()

        if not layer_name or low.startswith("gdb_") or low in {"gdb_items", "gdb_itemtypes"}:
            continue

        layer_kind = geom_kind_from_type(ogr.GT_Flatten(layer.GetGeomType()))
        layer_key = slug(layer_name)

        if should_skip_layer(meta["key"], layer_name, layer_kind):
            skipped.append({
                "package": package_name,
                "project": meta["name"],
                "gdb": gdb.name,
                "layer": layer_name,
                "geometry_kind": layer_kind,
                "reason": "filtered_helper_or_duplicate_layer"
            })
            continue

        transform = spatial_transform(layer)

        try:
            count_reported = layer.GetFeatureCount()
        except Exception:
            count_reported = -1

        exported = 0
        layer.ResetReading()

        try:
            for feat in layer:
                gj = feature_to_geojson(feat, layer, transform)
                if gj is None:
                    continue

                geom = feat.GetGeometryRef()
                if layer_kind == "other":
                    kind = geom_kind_from_geom(geom)
                else:
                    kind = layer_kind

                if kind == "other":
                    continue

                if should_skip_layer(meta["key"], layer_name, kind):
                    continue

                fid = int(feat.GetFID()) if feat.GetFID() is not None else exported
                gj = add_meta(gj, meta, package_name, gdb.name, layer_name, fid, kind)

                add_feature(store, seen, meta["key"], layer_key, kind, gj)
                exported += 1

        except Exception as exc:
            log(f"WARNING: layer partially skipped {layer_name}: {exc}")

        inventory.append({
            "package": package_name,
            "project": meta["name"],
            "project_key": meta["key"],
            "gdb": gdb.name,
            "layer": layer_name,
            "layer_key": layer_key,
            "geometry_kind": layer_kind,
            "feature_count_reported": count_reported,
            "feature_count_exported": exported,
            "default_visible": meta["defaultVisible"],
            "default_labels": meta["defaultLabels"],
        })

        log(f"  {meta['name']} / {layer_name}: {exported} feature(s)")


def write_geojson(path: Path, features: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    obj = {
        "type": "FeatureCollection",
        "features": features
    }
    path.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def build_and_write_outputs(
    store: Dict[Tuple[str, str, str], List[Dict[str, Any]]],
    inventory: List[Dict[str, Any]],
    skipped: List[Dict[str, Any]],
) -> None:
    project_meta_map: Dict[str, Dict[str, Any]] = {}

    for row in inventory:
        key = row["project_key"]
        if key not in project_meta_map:
            color = "#64748b"
            for _, rule_key, name, rule_color, visible, labels in PROJECT_RULES:
                if rule_key == key:
                    color = rule_color
                    break
            project_meta_map[key] = {
                "key": key,
                "name": row["project"],
                "color": color,
                "defaultVisible": bool(row.get("default_visible", True)),
                "defaultLabels": bool(row.get("default_labels", True)),
                "files": [],
                "featureCount": 0,
            }

    # Write one file per project/layer/kind.
    for (project_key, layer_key, kind), features in sorted(store.items()):
        if not features:
            continue

        file_name = f"{layer_key}_{kind}s.geojson"
        rel_path = f"data/map/projects/{project_key}/{file_name}"
        out_path = ROOT / rel_path

        write_geojson(out_path, features)

        project = project_meta_map.setdefault(project_key, {
            "key": project_key,
            "name": display(project_key),
            "color": color_for(project_key),
            "defaultVisible": True,
            "defaultLabels": True,
            "files": [],
            "featureCount": 0,
        })

        project["files"].append({
            "type": kind,
            "path": rel_path,
            "layerKey": layer_key,
            "layerName": display(layer_key),
            "count": len(features)
        })
        project["featureCount"] += len(features)

    manifest = {
        "version": 2,
        "mode": "split",
        "generatedBy": "tools/extract_project.py",
        "projects": sorted(project_meta_map.values(), key=lambda p: p["name"]),
        "totals": {
            "projects": len(project_meta_map),
            "files": sum(len(p["files"]) for p in project_meta_map.values()),
            "features": sum(p["featureCount"] for p in project_meta_map.values()),
        }
    }

    (MAP_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    inventory_fields = [
        "package", "project", "project_key", "gdb", "layer", "layer_key",
        "geometry_kind", "feature_count_reported", "feature_count_exported",
        "default_visible", "default_labels"
    ]

    skipped_fields = ["package", "project", "gdb", "layer", "geometry_kind", "reason"]

    write_csv(MAP_DIR / "layer_inventory.csv", inventory, inventory_fields)
    write_csv(MAP_DIR / "skipped_layers.csv", skipped, skipped_fields)

    log(f"Wrote split manifest with {manifest['totals']['files']} files and {manifest['totals']['features']} features.")


def main() -> int:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Fresh map output each run so old giant combined files are removed.
    if MAP_DIR.exists():
        shutil.rmtree(MAP_DIR)
    PROJECTS_DIR.mkdir(parents=True, exist_ok=True)

    # Remove old root compatibility files if present.
    for old_file in [
        ROOT / "data" / "structures_points.geojson",
        ROOT / "data" / "structures_polygons.geojson",
    ]:
        try:
            old_file.unlink()
        except FileNotFoundError:
            pass

    raw_files = sorted([
        p for p in RAW_DIR.iterdir()
        if p.is_file() and p.suffix.lower() in {".ppkx", ".mpkx", ".zip"}
    ])

    direct_gdbs = sorted([
        p for p in RAW_DIR.iterdir()
        if p.is_dir() and p.suffix.lower() == ".gdb"
    ])

    store: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
    seen: Dict[Tuple[str, str, str], set] = {}
    inventory: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    with tempfile.TemporaryDirectory(prefix="sgn_arcgis_extract_") as td:
        tmp = Path(td)

        for gdb in direct_gdbs:
            meta = project_meta(gdb)
            read_gdb(gdb, gdb.name, meta, store, seen, inventory, skipped)

        for src in raw_files:
            meta = project_meta(src)
            out = tmp / slug(src.name)
            out.mkdir(parents=True, exist_ok=True)

            if not extract_archive(src, out):
                continue

            gdbs = find_gdbs(out)

            if not gdbs:
                log(f"WARNING: no .gdb folders found inside {src.name}")
                continue

            for gdb in gdbs:
                read_gdb(gdb, src.name, meta, store, seen, inventory, skipped)

    build_and_write_outputs(store, inventory, skipped)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
