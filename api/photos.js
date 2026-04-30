const GH = 'https://api.github.com';

function cfg(body = {}, query = {}) {
  return {
    owner: body.owner || query.owner || process.env.GITHUB_OWNER,
    repo: body.repo || query.repo || process.env.GITHUB_REPO,
    branch: body.branch || query.branch || process.env.GITHUB_BRANCH || 'main',
    token: process.env.GITHUB_TOKEN,
  };
}
function send(res, status, data) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  return res.status(status).json(data);
}
function safeName(s) { return String(s || 'unknown').replace(/[^a-zA-Z0-9._-]+/g, '_').slice(0, 90); }
function utf8ToB64(s) { return Buffer.from(s, 'utf8').toString('base64'); }
function b64ToUtf8(s) { return Buffer.from(s || '', 'base64').toString('utf8'); }
function cleanB64(s) { return String(s || '').replace(/^data:[^,]+,/, ''); }
async function gh(c, path, opts = {}) {
  if (!c.token) throw new Error('Missing GITHUB_TOKEN environment variable.');
  if (!c.owner || !c.repo) throw new Error('Missing GITHUB_OWNER or GITHUB_REPO environment variable.');
  const r = await fetch(`${GH}${path}`, {
    ...opts,
    headers: {
      Accept: 'application/vnd.github+json',
      Authorization: `Bearer ${c.token}`,
      'X-GitHub-Api-Version': '2022-11-28',
      ...(opts.headers || {}),
    },
  });
  const text = await r.text();
  let data = {};
  try { data = text ? JSON.parse(text) : {}; } catch { data = { text }; }
  if (!r.ok) throw new Error(data.message || r.statusText);
  return data;
}
async function getContent(c, path) {
  try {
    const encoded = path.split('/').map(encodeURIComponent).join('/');
    return await gh(c, `/repos/${c.owner}/${c.repo}/contents/${encoded}?ref=${encodeURIComponent(c.branch)}`);
  } catch (e) {
    if (/not found/i.test(e.message)) return null;
    throw e;
  }
}
async function putContent(c, path, contentBase64, message, sha) {
  const encoded = path.split('/').map(encodeURIComponent).join('/');
  return gh(c, `/repos/${c.owner}/${c.repo}/contents/${encoded}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message, content: contentBase64, branch: c.branch, ...(sha ? { sha } : {}) }),
  });
}
async function getManifest(c) {
  const path = 'data/photos/manifest.json';
  const file = await getContent(c, path);
  if (!file) return { manifest: {}, sha: null };
  return { manifest: JSON.parse(b64ToUtf8(file.content)), sha: file.sha };
}
async function saveManifest(c, manifest, sha) {
  return putContent(c, 'data/photos/manifest.json', utf8ToB64(JSON.stringify(manifest, null, 2)), 'Update SGN map photo manifest', sha);
}

module.exports = async function handler(req, res) {
  if (req.method === 'OPTIONS') return send(res, 204, {});
  try {
    if (req.method === 'GET') {
      const c = cfg({}, req.query || {});
      const key = safeName(req.query.structureKey || req.query.key || '');
      const { manifest } = await getManifest(c);
      return send(res, 200, { photos: manifest[key] || [] });
    }
    if (req.method !== 'POST') return send(res, 405, { error: 'Method not allowed' });
    const body = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : (req.body || {});
    const c = cfg(body, req.query || {});
    const action = body.action || 'add';
    const key = safeName(body.structureKey || body.key || 'unknown');
    const { manifest, sha: manifestSha } = await getManifest(c);
    manifest[key] = Array.isArray(manifest[key]) ? manifest[key] : [];

    if (action === 'remove') {
      const photoId = body.photoId || body.id;
      manifest[key] = manifest[key].filter(p => p.id !== photoId);
      await saveManifest(c, manifest, manifestSha);
      return send(res, 200, { ok: true, photos: manifest[key] });
    }

    const rawName = body.filename || 'photo.jpg';
    const ext = (rawName.match(/\.[a-zA-Z0-9]+$/) || ['.jpg'])[0].toLowerCase();
    const safeFile = safeName(rawName).replace(/_+/g, '_');
    const id = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const path = `data/photos/${key}/${id}-${safeFile || ('photo' + ext)}`;
    const uploaded = await putContent(c, path, cleanB64(body.contentBase64), `Add SGN map photo for ${key}`);
    const photo = {
      id,
      filename: rawName,
      path,
      url: uploaded.content && uploaded.content.download_url,
      html_url: uploaded.content && uploaded.content.html_url,
      uploadedAt: new Date().toISOString(),
      uploadedBy: body.uploadedBy || '',
      mimeType: body.mimeType || '',
    };
    manifest[key].push(photo);
    await saveManifest(c, manifest, manifestSha);
    return send(res, 200, { ok: true, photo, photos: manifest[key] });
  } catch (e) {
    return send(res, 500, { error: e.message || String(e) });
  }
};
