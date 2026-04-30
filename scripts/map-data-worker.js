/* SGN Enterprise Fast Map Data Worker
   Fetches, caches, and parses GeoJSON away from the main UI thread.
   Output is still sent back to the page, but JSON.parse and cache reads happen in the worker.
*/
const DB_NAME = "sgn-map-worker-cache";
const STORE_NAME = "geojson";
const DB_VERSION = 1;

function openDb(){
  return new Promise((resolve,reject)=>{
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) db.createObjectStore(STORE_NAME, { keyPath:"key" });
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error || new Error("IndexedDB open failed"));
  });
}
async function idbGet(key){
  const db = await openDb();
  return new Promise((resolve,reject)=>{
    const tx = db.transaction(STORE_NAME, "readonly");
    const req = tx.objectStore(STORE_NAME).get(key);
    req.onsuccess = () => resolve(req.result || null);
    req.onerror = () => reject(req.error || new Error("IndexedDB read failed"));
  });
}
async function idbSet(record){
  const db = await openDb();
  return new Promise((resolve,reject)=>{
    const tx = db.transaction(STORE_NAME, "readwrite");
    tx.objectStore(STORE_NAME).put(record);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error || new Error("IndexedDB write failed"));
  });
}
async function idbClear(){
  const db = await openDb();
  return new Promise((resolve,reject)=>{
    const tx = db.transaction(STORE_NAME, "readwrite");
    tx.objectStore(STORE_NAME).clear();
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error || new Error("IndexedDB clear failed"));
  });
}
function keyFor(version,path){ return `${version}:${path}`; }
async function fetchText(path){
  let res = await fetch(path, { cache:"force-cache" });
  if (!res.ok && path.includes("data/optimized/")) {
    res = await fetch(path.replace("data/optimized/", "data/map/"), { cache:"force-cache" });
  }
  if (!res.ok) throw new Error(`${path} returned ${res.status}`);
  return await res.text();
}
function trimFeatureCollection(data){
  if (!data || data.type !== "FeatureCollection" || !Array.isArray(data.features)) return data;
  // Keep the GeoJSON structure intact. This hook is here so future filters/simplifiers can be added
  // without touching index.html.
  return data;
}
async function loadJson(path, version){
  const key = keyFor(version, path);
  const cached = await idbGet(key);
  if (cached && cached.text) {
    return trimFeatureCollection(JSON.parse(cached.text));
  }
  const text = await fetchText(path);
  // Validate parse before writing cache.
  const data = trimFeatureCollection(JSON.parse(text));
  try { await idbSet({ key, path, version, text, savedAt:Date.now() }); } catch(_) {}
  return data;
}
self.onmessage = async (event) => {
  const msg = event.data || {};
  try{
    if (msg.type === "load-json") {
      const data = await loadJson(msg.path, msg.version || "default");
      self.postMessage({ id:msg.id, ok:true, data });
      return;
    }
    if (msg.type === "clear-cache") {
      await idbClear();
      self.postMessage({ id:msg.id, ok:true, data:{ cleared:true } });
      return;
    }
    self.postMessage({ id:msg.id, ok:false, error:"Unknown worker message type" });
  }catch(err){
    self.postMessage({ id:msg.id, ok:false, error:err && err.message ? err.message : String(err) });
  }
};
