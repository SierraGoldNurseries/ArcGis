# SGN Structures App v7

This version adds the requested app-like map upgrades:

- Structure details drawer / mobile bottom sheet
- Export PNG, print map, and export selected structures to PDF
- Viewer mode and Admin mode toggle
- Better mobile UX with larger controls, GPS follow, full-screen, and offline caching
- Structure type badges and grouped colors
- Open in Google Maps / Apple Maps / directions / copy coordinates
- Photo attachments per structure (saved locally in the browser)
- Existing backend import workflow remains in `import.html`

## Pages

- `index.html` — live map app
- `import.html` — project builder / upload page

## Notes

Photo attachments are currently stored in browser local storage for the selected device/browser.
For shared team-wide photo storage, wire the upload flow to your backend or a database service.

## Shared photos for all users

Version 8 adds shared photo storage. Photos attached to a structure are saved through the backend into the GitHub repository under:

```text
data/photos/<structure-key>/
data/photos/manifest.json
```

This requires deploying with Vercel or Netlify because GitHub Pages cannot run backend functions.

Required environment variables:

```text
GITHUB_TOKEN       fine-grained token with Contents: Read and Write
GITHUB_OWNER       repository owner
GITHUB_REPO        repository name
GITHUB_BRANCH      usually main
```

Endpoints:

```text
Vercel:  /api/photos
Netlify: /.netlify/functions/photos
```

If the backend is not configured, photo uploads fall back to local browser storage so the map still works, but those fallback photos are only visible on that device.

Recommended next UI upgrades:

- Add status fields per structure: Active, Needs Repair, Verified, Inactive.
- Add before/after import comparison with Added, Removed, Changed, and Moved structures.
- Add user login and audit history for photo/status changes.
- Add QR codes for each structure so staff can scan and open the exact map feature.
- Add task/work-order style checklists for repairs or inspections.
- Add a version timeline to switch between older ArcGIS imports.
- Add offline photo queue for field work when cellular service is weak.
- Add drawing/markup tools on photos so users can circle issues.
