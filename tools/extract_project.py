#!/usr/bin/env python3
"""
SGN ArcGIS extractor - tolerant version.

Fix:
- Skips unreadable/broken .gdb folders instead of failing the whole workflow.
- Exports all readable layers from every .ppkx/.mpkx/.zip in data/raw.
- Writes combined map data to data/map.
"""

from __future__ import annotations

import csv
import json
import re
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from osgeo import ogr, osr, gdal
except Exception as exc:
    print("ERROR: Python GDAL bindings are missing:", exc)
    sys.exit(2)

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
MAP_DIR = ROOT / "data" / "map"

PROJECT_RULES = [
    ("chemical", "Chemical Locations", "#dc2626", True, True),
    ("irrigation", "Irrigation", "#2563eb", True, False),
    ("owl", "Owl Boxes", "#7c3aed", True, False),
    ("tree", "Trees", "#16a34a", True, False),
    ("prune_north", "Prune North", "#9333ea", True, False),
    ("prunenorth", "Prune North", "#9333ea", True, False),
    ("prune_south", "Prune South", "#c026d3", True, False),
    ("prunesouth", "Prune South", "#c026d3", True, False),
    ("railroad_south", "Railroad South", "#0f766e", True, False),
    ("railroadsouth", "Railroad South", "#0f766e", True, False),
    ("block_#23", "Block #23", "#f59e0b", True, True),
    ("block_23", "Block #23", "#f59e0b", True, True),
    ("block23", "Block #23", "#f59e0b", True, True),
    ("block_#25", "Block #25", "#ea580c", True, True),
    ("block_25", "Block #25", "#ea580c", True, True),
    ("block25", "Block #25", "#ea580c", True, True),
    ("structure", "Structures / Yards", "#0f766e", True, True),
    ("yard", "Structures / Yards", "#0f766e", True, True),
]

COLOR_PALETTE = [
    "#0f766e", "#0284c7", "#7c3aed", "#b45309", "#dc2626",
    "#0891b2", "#4f46e5", "#be185d", "#15803d", "#9333ea",
    "#64748b", "#f59e0b", "#ea580c", "#14b8a6", "#2563eb"
]


def log(msg: str) -> None:
    print(f"[extract] {msg}", flush=True)


def clean(value: Any) -> str:
    s = Path(str(value)).stem
    s = re.sub(r"[\s\-]+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_#]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


def human(value: str) -> str:
    return clean(value).replace("_", " ")


def project_meta(path: Path) -> Dict[str, Any]:
    key = clean(path.name).lower()
    compact = key.replace("_", "").replace("#", "")
    for token, name, color, visible, labels in PROJECT_RULES:
        t1 = token.lower()
        t2 = t1.replace("_", "").replace("#", "")
        if t1 in key or t2 in compact:
            return {
                "project": name,
                "project_key": clean(name).lower(),
                "project_color": color,
                "default_visible": visible,
                "default_labels": labels,
            }
    return {
        "project": "Other",
        "project_key": "other",
        "project_color": "#64748b",
        "default_visible": True,
        "default_labels": False,
    }


def group_key(layer_name: str, project_name: str) -> str:
    n = clean(layer_name)

    patterns = [
        r"^(High_Tunnels_\d+)(?:_[A-Za-z])?$",
        r"^(Can_Yard_\d+)(?:_[A-Za-z])?$",
        r"^(Shade_House_\d+|Shadehouse_\d+)(?:_[A-Za-z])?$",
        r"^(Block_#?\d+).*",
    ]
    for pat in patterns:
        m = re.match(pat, n, flags=re.I)
        if m:
            return clean(m.group(1)).lower()

    if re.match(r"^Four_Bay[s]?_\d+$", n, flags=re.I):
        return "four-bays"
    if re.match(r"^Cold_Frame_\d+$", n, flags=re.I):
        return "cold-frame"

    return f"{clean(project_name).lower()}::{clean(layer_name).lower()}"


def color_for(key: str) -> str:
    h = 0
    for ch in key:
        h = ((h << 5) - h + ord(ch)) & 0xFFFFFFFF
    return COLOR_PALETTE[h % len(COLOR_PALETTE)]


def layer_label(layer_name: str) -> str:
    n = clean(layer_name)

    m = re.match(r"Four_Bay[s]?_(\d+)$", n, re.I)
    if m:
        return f"FB{int(m.group(1)):02d}"

    m = re.match(r"Can_Yard_(\d+)(?:_([A-Za-z]))?$", n, re.I)
    if m:
        return f"CY{int(m.group(1)):02d}{(m.group(2) or '').upper()}"

    m = re.match(r"High_Tunnels_(\d+)(?:_([A-Za-z]))?$", n, re.I)
    if m:
        return f"HT{int(m.group(1)):02d}{(m.group(2) or '').upper()}"

    m = re.match(r"Shade(?:_House|house)_(\d+)(?:_([A-Za-z]))?$", n, re.I)
    if m:
        return f"SH{int(m.group(1)):02d}{(m.group(2) or '').upper()}"

    m = re.match(r"Cold_Frame_(\d+)$", n, re.I)
    if m:
        return f"CF{int(m.group(1)):02d}"

    m = re.match(r"Block_#?(\d+)", n, re.I)
    if m:
        return f"BLK{int(m.group(1)):02d}"

    return human(layer_name)


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
        log(f"7z extraction failed for {src.name}: {exc}. Trying zipfile...")

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


def geom_kind(flat_type: int) -> str:
    if flat_type in (ogr.wkbPolygon, ogr.wkbMultiPolygon):
        return "polygon"
    if flat_type in (ogr.wkbPoint, ogr.wkbMultiPoint):
        return "point"
    if flat_type in (ogr.wkbLineString, ogr.wkbMultiLineString):
        return "line"
    return "other"


def feature_to_geojson(feat: ogr.Feature, layer: ogr.Layer, transform: Optional[osr.CoordinateTransformation]) -> Optional[Dict[str, Any]]:
    geom = feat.GetGeometryRef()
    if geom is None or geom.IsEmpty():
        return None

    geom = geom.Clone()
    if transform is not None:
        try:
            geom.Transform(transform)
        except Exception as exc:
            log(f"WARNING: skipped feature due to coordinate transform error: {exc}")
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
            if isinstance(value, bytes):
                value = value.decode("utf-8", errors="replace")
            props[name] = value
        except Exception:
            pass

    return {"type": "Feature", "geometry": geom_json, "properties": props}


def point_from_feature(feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        geom = ogr.CreateGeometryFromJson(json.dumps(feature["geometry"]))
        if geom is None or geom.IsEmpty():
            return None
        try:
            pt = geom.PointOnSurface()
        except Exception:
            pt = geom.Centroid()
        if pt is None or pt.IsEmpty():
            pt = geom.Centroid()
        return {
            "type": "Feature",
            "geometry": json.loads(pt.ExportToJson()),
            "properties": dict(feature["properties"]),
        }
    except Exception:
        return None


def add_meta(
    feature: Dict[str, Any],
    meta: Dict[str, Any],
    package_name: str,
    gdb_name: str,
    layer_name: str,
    fid: int,
    kind: str,
) -> Dict[str, Any]:
    gkey = group_key(layer_name, meta["project"])
    props = feature.setdefault("properties", {})
    props["_project"] = meta["project"]
    props["_project_key"] = meta["project_key"]
    props["_project_color"] = meta["project_color"]
    props["_default_visible"] = meta["default_visible"]
    props["_default_labels"] = meta["default_labels"]
    props["_package"] = package_name
    props["_gdb"] = gdb_name
    props["_layer"] = layer_name
    props["_layer_display"] = human(layer_name)
    props["_label"] = layer_label(layer_name)
    props["_group_key"] = gkey
    props["_group_color"] = color_for(gkey)
    props["_fid"] = fid
    props["_geometry_kind"] = kind
    props["_feature_key"] = f"{meta['project_key']}::{clean(layer_name).lower()}::{fid}"
    return feature


def read_gdb(
    gdb: Path,
    package_name: str,
    meta: Dict[str, Any],
    polygons: List[Dict[str, Any]],
    points: List[Dict[str, Any]],
    lines: List[Dict[str, Any]],
    inventory: List[Dict[str, Any]],
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

    layer_count = ds.GetLayerCount()
    if layer_count <= 0:
        log(f"WARNING: skipping empty geodatabase {gdb}")
        return

    for i in range(layer_count):
        layer = ds.GetLayerByIndex(i)
        if layer is None:
            continue

        layer_name = layer.GetName()
        if not layer_name or layer_name.startswith("GDB_") or layer_name.lower() in {"gdb_items", "gdb_itemtypes"}:
            continue

        flat = ogr.GT_Flatten(layer.GetGeomType())
        kind = geom_kind(flat)
        if kind == "other":
            continue

        try:
            count_reported = layer.GetFeatureCount()
        except Exception:
            count_reported = -1

        transform = spatial_transform(layer)
        exported = 0
        layer.ResetReading()

        try:
            for feat in layer:
                gj = feature_to_geojson(feat, layer, transform)
                if gj is None:
                    continue
                fid = int(feat.GetFID()) if feat.GetFID() is not None else exported
                gj = add_meta(gj, meta, package_name, gdb.name, layer_name, fid, kind)

                if kind == "polygon":
                    polygons.append(gj)
                    pt = point_from_feature(gj)
                    if pt is not None:
                        pt = add_meta(pt, meta, package_name, gdb.name, layer_name, fid, "polygon_point")
                        points.append(pt)
                elif kind == "point":
                    points.append(gj)
                elif kind == "line":
                    lines.append(gj)
                    pt = point_from_feature(gj)
                    if pt is not None:
                        pt = add_meta(pt, meta, package_name, gdb.name, layer_name, fid, "line_point")
                        points.append(pt)

                exported += 1

        except Exception as exc:
            log(f"WARNING: layer failed and was partially skipped: {layer_name}: {exc}")

        inventory.append({
            "package": package_name,
            "project": meta["project"],
            "gdb": gdb.name,
            "layer": layer_name,
            "geometry_kind": kind,
            "feature_count_reported": count_reported,
            "feature_count_exported": exported,
            "default_visible": meta["default_visible"],
            "default_labels": meta["default_labels"],
        })

        log(f"  {meta['project']} / {layer_name}: {exported} {kind} features")


def write_geojson(path: Path, features: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    obj = {"type": "FeatureCollection", "features": features}
    path.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    log(f"Wrote {path.relative_to(ROOT)} ({len(features)} features)")


def write_inventory(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "package", "project", "gdb", "layer", "geometry_kind",
        "feature_count_reported", "feature_count_exported",
        "default_visible", "default_labels"
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({field: row.get(field, "") for field in fields})
    log(f"Wrote {path.relative_to(ROOT)}")


def build_manifest(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    projects: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        name = row["project"]
        key = clean(name).lower()
        if key not in projects:
            color = next((rule[2] for rule in PROJECT_RULES if rule[1] == name), "#64748b")
            projects[key] = {
                "key": key,
                "name": name,
                "color": color,
                "default_visible": bool(row.get("default_visible", True)),
                "default_labels": bool(row.get("default_labels", False)),
                "feature_count": 0,
                "layers": [],
            }
        projects[key]["feature_count"] += int(row.get("feature_count_exported") or 0)
        projects[key]["layers"].append({
            "layer": row["layer"],
            "geometry_kind": row["geometry_kind"],
            "feature_count": row["feature_count_exported"],
        })

    return {
        "generated_by": "tools/extract_project.py",
        "total_projects": len(projects),
        "total_layers": len(rows),
        "projects": list(projects.values()),
    }


def build_config(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "data_files": {
            "polygons": "data/map/combined_polygons.geojson",
            "points": "data/map/combined_points.geojson",
            "lines": "data/map/combined_lines.geojson",
            "manifest": "data/map/project_manifest.json",
            "inventory": "data/map/layer_inventory.csv",
        },
        "project_defaults": {
            "Structures / Yards": {"labels": True, "visible": True},
            "Chemical Locations": {"labels": True, "visible": True},
            "Irrigation": {"labels": False, "visible": True, "show_labels_when_selected": True},
            "Owl Boxes": {"labels": False, "visible": True},
            "Trees": {"labels": False, "visible": True, "cluster": True, "search": True},
            "Prune North": {"labels": False, "visible": True, "filtered_view": True},
            "Prune South": {"labels": False, "visible": True, "filtered_view": True},
            "Railroad South": {"labels": False, "visible": True, "filtered_view": True},
            "Block #23": {"labels": True, "visible": True, "opacity": 0.16},
            "Block #25": {"labels": True, "visible": True, "opacity": 0.16},
        },
        "layers": rows,
    }


def main() -> int:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    MAP_DIR.mkdir(parents=True, exist_ok=True)

    raw_files = sorted([
        p for p in RAW_DIR.iterdir()
        if p.is_file() and p.suffix.lower() in {".ppkx", ".mpkx", ".zip"}
    ])
    direct_gdbs = sorted([p for p in RAW_DIR.iterdir() if p.is_dir() and p.suffix.lower() == ".gdb"])

    if not raw_files and not direct_gdbs:
        log("No raw files found in data/raw. Writing empty outputs.")
        write_geojson(MAP_DIR / "combined_polygons.geojson", [])
        write_geojson(MAP_DIR / "combined_points.geojson", [])
        write_geojson(MAP_DIR / "combined_lines.geojson", [])
        return 0

    polygons: List[Dict[str, Any]] = []
    points: List[Dict[str, Any]] = []
    lines: List[Dict[str, Any]] = []
    inventory: List[Dict[str, Any]] = []

    with tempfile.TemporaryDirectory(prefix="sgn_arcgis_extract_") as td:
        tmp = Path(td)

        for gdb in direct_gdbs:
            read_gdb(gdb, gdb.name, project_meta(gdb), polygons, points, lines, inventory)

        for src in raw_files:
            meta = project_meta(src)
            out = tmp / clean(src.name)
            out.mkdir(parents=True, exist_ok=True)

            if not extract_archive(src, out):
                continue

            gdbs = find_gdbs(out)
            if not gdbs:
                log(f"WARNING: no .gdb folders found inside {src.name}")
                continue

            for gdb in gdbs:
                read_gdb(gdb, src.name, meta, polygons, points, lines, inventory)

    write_geojson(MAP_DIR / "combined_polygons.geojson", polygons)
    write_geojson(MAP_DIR / "combined_points.geojson", points)
    write_geojson(MAP_DIR / "combined_lines.geojson", lines)

    # compatibility for older index.html versions
    write_geojson(ROOT / "data" / "structures_polygons.geojson", polygons)
    write_geojson(ROOT / "data" / "structures_points.geojson", points)

    write_inventory(MAP_DIR / "layer_inventory.csv", inventory)
    (MAP_DIR / "project_manifest.json").write_text(json.dumps(build_manifest(inventory), indent=2), encoding="utf-8")
    (MAP_DIR / "map_config.json").write_text(json.dumps(build_config(inventory), indent=2), encoding="utf-8")

    log(f"Done. Exported {len(polygons)} polygons, {len(points)} points, {len(lines)} lines.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
