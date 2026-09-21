const CACHE="fast-messenger-shell-v8";
const RUNTIME="fast-messenger-runtime-v8";
const SHELL=["./","./index.html","./assets/js/app.js","./assets/theme-background.webp"];
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
