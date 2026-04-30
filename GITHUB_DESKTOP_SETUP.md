# SGN Map App - GitHub Desktop Setup

## One-time setup

1. Open GitHub Desktop.
2. Select `SierraGoldNurseries/ArcGis` from the left list.
3. Click the blue button at the bottom: **Clone SierraGoldNurseries/ArcGis**.
4. Choose a local folder, for example:

```text
Documents/GitHub/ArcGis
```

5. Click **Clone**.

## Replace the app files from this ZIP

1. Extract this ZIP on your computer.
2. Open the extracted folder.
3. Copy everything inside it.
4. Open your cloned repo folder from GitHub Desktop: `Repository > Show in Explorer`.
5. Paste/replace the files in the repo folder.
6. In GitHub Desktop, review the changes.
7. Enter a summary like:

```text
Replace SGN map app v12
```

8. Click **Commit to main**.
9. Click **Push origin**.

## Upload large `.ppkx` files with GitHub Desktop

Do not upload large `.ppkx` files through the GitHub website. Use GitHub Desktop instead.

1. In GitHub Desktop, open `Repository > Show in Explorer`.
2. Go to:

```text
data/raw/
```

3. Drag your `.ppkx` files into `data/raw/`.
4. Go back to GitHub Desktop.
5. Add a commit message like:

```text
Upload ArcGIS project packages
```

6. Click **Commit to main**.
7. Click **Push origin**.

## What happens after push

The GitHub Action watches `data/raw/**`. After you push `.ppkx` files, it runs the extractor and writes generated map data into:

```text
data/map/
  combined_polygons.geojson
  combined_points.geojson
  structures_polygons.geojson
  structures_points.geojson
  project_manifest.json
  map_config.json
  layer_inventory.csv
```

Then Vercel redeploys the website.

## Large file warning

GitHub Desktop can push files larger than the GitHub website upload limit. GitHub website upload limit is 25 MB. Standard GitHub blocks files above 100 MB unless you use Git LFS.

Your `Trees_Backup_04_01_2026.ppkx` is about 75 MB, so GitHub Desktop should be okay. If a future file is over 100 MB, install Git LFS first.
