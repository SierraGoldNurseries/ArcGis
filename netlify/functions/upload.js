// Netlify function equivalent of api/upload.js.
// Env vars: GITHUB_TOKEN, GITHUB_OWNER, GITHUB_REPO, optional GITHUB_BRANCH.
const GH='https://api.github.com';
exports.handler=async(event)=>{
  if(event.httpMethod!=='POST')return json(405,{error:'POST only'});
  try{
    const body=JSON.parse(event.body||'{}');
    const filename=cleanName(body.filename||'upload.bin');
    const contentBase64=body.contentBase64;
    if(!contentBase64)return json(400,{error:'Missing contentBase64'});
    const owner=process.env.GITHUB_OWNER||body.owner;
    const repo=process.env.GITHUB_REPO||body.repo;
    const branch=process.env.GITHUB_BRANCH||body.branch||'main';
    const token=process.env.GITHUB_TOKEN;
    if(!token)return json(500,{error:'Server missing GITHUB_TOKEN env var'});
    if(!owner||!repo)return json(400,{error:'Missing owner/repo'});
    const path=`data/raw/${Date.now()}_${filename}`;
    const put=await gh(`/repos/${owner}/${repo}/contents/${path}`,token,{method:'PUT',body:JSON.stringify({message:`Upload ArcGIS project file: ${filename}`,content:contentBase64,branch})});
    if(!put.ok)return json(put.status,{error:await put.text()});
    await gh(`/repos/${owner}/${repo}/dispatches`,token,{method:'POST',body:JSON.stringify({event_type:'arcgis_upload',client_payload:{path,filename,branch}})}).catch(()=>null);
    return json(200,{ok:true,message:`Uploaded ${filename}. Build should start automatically.`,path});
  }catch(e){return json(500,{error:e.message||String(e)})}
};
function json(statusCode,obj){return{statusCode,headers:{'Content-Type':'application/json'},body:JSON.stringify(obj)}}
function cleanName(name){return String(name).replace(/[^A-Za-z0-9._-]+/g,'_').slice(-160)}
async function gh(path,token,init={}){return fetch(`${GH}${path}`,{...init,headers:{Authorization:`Bearer ${token}`,Accept:'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28',...(init.headers||{})}})}
