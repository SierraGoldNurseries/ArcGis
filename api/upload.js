// Vercel/Node serverless function.
// Stores an uploaded project file in data/raw/ and triggers the ArcGIS extractor workflow.
// Required env vars: GITHUB_TOKEN, GITHUB_OWNER, GITHUB_REPO. Optional: GITHUB_BRANCH=main.

const GH = 'https://api.github.com';

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST only' });
  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    const filename = cleanName(body.filename || 'upload.bin');
    const contentBase64 = body.contentBase64;
    if (!contentBase64) return res.status(400).json({ error: 'Missing contentBase64' });

    const owner = process.env.GITHUB_OWNER || body.owner;
    const repo = process.env.GITHUB_REPO || body.repo;
    const branch = process.env.GITHUB_BRANCH || body.branch || 'main';
    const token = process.env.GITHUB_TOKEN;
    if (!token) return res.status(500).json({ error: 'Server missing GITHUB_TOKEN env var' });
    if (!owner || !repo) return res.status(400).json({ error: 'Missing owner/repo. Set env vars or pass them from the page.' });

    const path = `data/raw/${Date.now()}_${filename}`;
    const put = await gh(`/repos/${owner}/${repo}/contents/${path}`, token, {
      method: 'PUT',
      body: JSON.stringify({
        message: `Upload ArcGIS project file: ${filename}`,
        content: contentBase64,
        branch
      })
    });
    if (!put.ok) return res.status(put.status).json({ error: await put.text() });

    // Push trigger should run automatically, but repository_dispatch gives a second nudge.
    await gh(`/repos/${owner}/${repo}/dispatches`, token, {
      method: 'POST',
      body: JSON.stringify({ event_type: 'arcgis_upload', client_payload: { path, filename, branch } })
    }).catch(() => null);

    return res.status(200).json({ ok: true, message: `Uploaded ${filename}. Build should start automatically.`, path });
  } catch (err) {
    return res.status(500).json({ error: err.message || String(err) });
  }
};

function cleanName(name) {
  return String(name).replace(/[^A-Za-z0-9._-]+/g, '_').slice(-160);
}
async function gh(path, token, init = {}) {
  return fetch(`${GH}${path}`, {
    ...init,
    headers: {
      'Authorization': `Bearer ${token}`,
      'Accept': 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      ...(init.headers || {})
    }
  });
}
