# SGN Map App v10 - Project Layer Defaults

This version includes the requested project structure and label/display behavior:

- Structures / Yards
- Chemical Locations
- Irrigation
- Owl Boxes
- Trees
- Prune North
- Prune South
- Block #23
- Block #25

## Default map behavior

- Structures / Yards: labels on by default
- Chemical Locations: labels on by default
- Irrigation: labels off by default; selected feature label is shown in the drawer workflow
- Trees: labels off by default; point layer uses clustering/search
- Prune North / Prune South: labels off by default; use project filter/search
- Block #23 / Block #25: labels on by default; lower polygon opacity

## Duplicate labels

Labels are de-duplicated by project + source layer, so the app does not show both a polygon label and a generated point label for the same feature group.

## Import

Use `import.html` or the visible **Admin / Import** button. For `.ppkx` project packages, extraction runs in the backend/GitHub Action because browsers cannot directly read ArcGIS File Geodatabases.
