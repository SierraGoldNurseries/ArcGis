// Netlify function equivalent of api/status.js.
const GH='https://api.github.com';
exports.handler=async(event)=>{
  if(event.httpMethod!=='GET')return json(405,{error:'GET only'});
  try{
    const q=event.queryStringParameters||{};
    const owner=process.env.GITHUB_OWNER||q.owner;
    const repo=process.env.GITHUB_REPO||q.repo;
    const branch=process.env.GITHUB_BRANCH||q.branch||'main';
    const token=process.env.GITHUB_TOKEN;
    if(!token)return json(500,{error:'Server missing GITHUB_TOKEN env var'});
    if(!owner||!repo)return json(400,{error:'Missing owner/repo'});
    const r=await fetch(`${GH}/repos/${owner}/${repo}/actions/workflows/extract-arcgis.yml/runs?branch=${encodeURIComponent(branch)}&per_page=1`,{headers:{Authorization:`Bearer ${token}`,Accept:'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'}});
    if(!r.ok)return json(r.status,{error:await r.text()});
    const data=await r.json(); const run=data.workflow_runs&&data.workflow_runs[0];
    if(!run)return json(200,{status:'none',conclusion:null});
    return json(200,{status:run.status,conclusion:run.conclusion,html_url:run.html_url,updated_at:run.updated_at});
  }catch(e){return json(500,{error:e.message||String(e)})}
};
function json(statusCode,obj){return{statusCode,headers:{'Content-Type':'application/json'},body:JSON.stringify(obj)}}
