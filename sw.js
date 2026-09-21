const CACHE="fast-messenger-shell-v7";
const RUNTIME="fast-messenger-runtime-v7";
const SHELL=["./","./index.html","./assets/css/app.css","./assets/js/app.js","./manifest.webmanifest","./icon.svg"];
self.addEventListener("install",e=>e.waitUntil(caches.open(CACHE).then(c=>c.addAll(SHELL)).then(()=>self.skipWaiting())));
self.addEventListener("activate",e=>e.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>![CACHE,RUNTIME].includes(k)).map(k=>caches.delete(k)))).then(()=>self.clients.claim())));
self.addEventListener("fetch",e=>{
  if(e.request.method!=="GET")return;
  const u=new URL(e.request.url);
  if(u.origin!==location.origin)return;
  e.respondWith(caches.match(e.request).then(cached=>{
    const network=fetch(e.request).then(r=>{if(r.ok)caches.open(RUNTIME).then(c=>c.put(e.request,r.clone()));return r}).catch(()=>cached||caches.match("./index.html"));
    return cached||network;
  }));
});