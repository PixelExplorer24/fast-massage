const CACHE="fast-messenger-shell-v16";
const RUNTIME="fast-messenger-runtime-v16";
const SHELL=["./","./index.html","./assets/js/app.js","./assets/theme-background.webp","./assets/logo.webp"];

self.addEventListener("install",e=>e.waitUntil(
  caches.open(CACHE).then(c=>c.addAll(SHELL)).then(()=>self.skipWaiting())
));

self.addEventListener("activate",e=>e.waitUntil(
  caches.keys().then(keys=>Promise.all(
    keys.filter(k=>![CACHE,RUNTIME].includes(k)).map(k=>caches.delete(k))
  )).then(()=>self.clients.claim())
));

self.addEventListener("fetch",e=>{
  if(e.request.method!=="GET")return;

  const u=new URL(e.request.url);

  // Profile/chat images may come from Firebase Storage or another CDN. Cache
  // successful image responses so the next hard refresh can paint the avatar
  // immediately instead of waiting for the remote image request again.
  if(e.request.destination==="image"){
    e.respondWith(
      caches.match(e.request).then(cached=>{
        const network=fetch(e.request).then(r=>{
          if(r.ok || r.type==="opaque"){
            caches.open(RUNTIME).then(c=>c.put(e.request,r.clone())).catch(()=>{});
          }
          return r;
        }).catch(()=>cached||Response.error());
        return cached||network;
      })
    );
    return;
  }

  if(u.origin!==location.origin)return;

  const isShell=u.pathname.endsWith("/index.html")||
    u.pathname.endsWith("/assets/js/app.js")||
    u.pathname.endsWith("/sw.js");

  if(isShell){
    e.respondWith(
      fetch(e.request,{cache:"no-store"}).then(r=>{
        if(r.ok)caches.open(CACHE).then(c=>c.put(e.request,r.clone())).catch(()=>{});
        return r;
      }).catch(()=>caches.match(e.request).then(r=>r||caches.match("./index.html")))
    );
    return;
  }

  e.respondWith(
    caches.match(e.request).then(cached=>{
      const network=fetch(e.request).then(r=>{
        if(r.ok)caches.open(RUNTIME).then(c=>c.put(e.request,r.clone())).catch(()=>{});
        return r;
      }).catch(()=>cached||caches.match("./index.html"));
      return cached||network;
    })
  );
});
