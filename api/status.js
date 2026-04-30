// Vercel/Node serverless function.
// Returns latest ArcGIS extractor workflow run status.
// Required env vars: GITHUB_TOKEN, GITHUB_OWNER, GITHUB_REPO. Optional: GITHUB_BRANCH=main.

const GH = 'https://api.github.com';
module.exports = async function handler(req, res) {
  if (req.method !== 'GET') return res.status(405).json({ error: 'GET only' });
  try {
    const owner = process.env.GITHUB_OWNER || req.query.owner;
    const repo = process.env.GITHUB_REPO || req.query.repo;
    const branch = process.env.GITHUB_BRANCH || req.query.branch || 'main';
    const token = process.env.GITHUB_TOKEN;
    if (!token) return res.status(500).json({ error: 'Server missing GITHUB_TOKEN env var' });
    if (!owner || !repo) return res.status(400).json({ error: 'Missing owner/repo' });
    const path = `/repos/${owner}/${repo}/actions/workflows/extract-arcgis.yml/runs?branch=${encodeURIComponent(branch)}&per_page=1`;
    const r = await fetch(`${GH}${path}`, { headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28' } });
    if (!r.ok) return res.status(r.status).json({ error: await r.text() });
    const data = await r.json();
    const run = data.workflow_runs && data.workflow_runs[0];
    if (!run) return res.status(200).json({ status: 'none', conclusion: null });
    return res.status(200).json({ status: run.status, conclusion: run.conclusion, html_url: run.html_url, updated_at: run.updated_at });
  } catch (err) {
    return res.status(500).json({ error: err.message || String(err) });
  }
};
