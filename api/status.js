function send(res, status, data) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(data));
}

async function gh(path, token) {
  const res = await fetch("https://api.github.com" + path, {
    headers: {
      "Accept": "application/vnd.github+json",
      "Authorization": "Bearer " + token,
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "sgn-map-import-status"
    }
  });

  const text = await res.text();
  let json = {};
  try {
    json = text ? JSON.parse(text) : {};
  } catch (err) {
    json = { raw: text };
  }

  if (!res.ok) {
    throw new Error(json.message || json.error || text || ("GitHub error " + res.status));
  }

  return json;
}

module.exports = async (req, res) => {
  try {
    if (req.method !== "GET" && req.method !== "POST") {
      return send(res, 405, { ok: false, error: "Method not allowed" });
    }

    const body = req.method === "POST"
      ? (typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {}))
      : {};

    const input = Object.assign({}, req.query || {}, body || {});
    const adminPin = input.adminPin || req.headers["x-admin-pin"] || "";
    const envPin = process.env.ADMIN_PIN || "";

    if (envPin && adminPin !== envPin) {
      return send(res, 401, { ok: false, error: "Invalid admin PIN" });
    }

    const owner = input.owner || process.env.GITHUB_OWNER;
    const repo = input.repo || process.env.GITHUB_REPO;
    const branch = input.branch || process.env.GITHUB_BRANCH || "main";
    const workflow = input.workflow || "extract-arcgis.yml";

    const token =
      input.githubToken ||
      input.token ||
      req.headers["x-github-token"] ||
      process.env.GITHUB_TOKEN ||
      "";

    if (!owner || !repo) {
      return send(res, 400, { ok: false, error: "Missing owner or repo" });
    }

    if (!token) {
      return send(res, 400, {
        ok: false,
        error: "Missing GitHub token. Paste it in the import page and click Save Settings, or set GITHUB_TOKEN in Vercel."
      });
    }

    const runs = await gh(
      "/repos/" + encodeURIComponent(owner) + "/" + encodeURIComponent(repo) +
      "/actions/workflows/" + encodeURIComponent(workflow) +
      "/runs?branch=" + encodeURIComponent(branch) + "&per_page=5",
      token
    );

    const run = (runs.workflow_runs || [])[0] || null;

    return send(res, 200, {
      ok: true,
      run: run ? {
        id: run.id,
        name: run.name,
        status: run.status,
        conclusion: run.conclusion,
        html_url: run.html_url,
        created_at: run.created_at,
        updated_at: run.updated_at,
        actor: run.actor ? run.actor.login : null
      } : null
    });
  } catch (err) {
    return send(res, 500, {
      ok: false,
      error: err.message || "Status endpoint failed"
    });
  }
};
