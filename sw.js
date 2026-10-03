const CACHE="fast-messenger-shell-v24";
const RUNTIME="fast-messenger-runtime-v24";
const SHELL=["./","./index.html","./assets/css/app.css","./assets/js/app.js","./assets/theme-background.webp","./assets/logo.webp","./icon.svg"];

function isNativeWebView(){
  const ua=String(self.navigator?.userAgent||"");
  return /;\s*wv\)|\bwv\b|ReactNative|Expo/i.test(ua)||(/iPhone|iPad|iPod/i.test(ua)&&!/Safari/i.test(ua));
}

// FCM background messaging for Chrome/PWA. The native Expo WebView uses its
// existing native push bridge and must not also show web notifications.
try{
  importScripts("https://www.gstatic.com/firebasejs/10.14.1/firebase-app-compat.js");
  importScripts("https://www.gstatic.com/firebasejs/10.14.1/firebase-messaging-compat.js");
  firebase.initializeApp({
    apiKey:"AIzaSyDc-XZGguSKRxZUIG6s5h0sjhcuJrtZQZc",
    authDomain:"fast-massage-3ac80.firebaseapp.com",
    databaseURL:"https://fast-massage-3ac80-default-rtdb.firebaseio.com",
    projectId:"fast-massage-3ac80",
    storageBucket:"fast-massage-3ac80.firebasestorage.app",
    messagingSenderId:"114872273052",
    appId:"1:114872273052:web:f0873cc1f474b884ef4559"
  });
  const fcmMessaging=firebase.messaging();
  fcmMessaging.onBackgroundMessage(payload=>{
    if(isNativeWebView())return;
    const n=payload.notification||{},d=payload.data||{};
    const title=n.title||d.title||"Fast Messenger";
    const body=n.body||d.body||"নতুন notification এসেছে";
    const target=d.url||d.route||"./";
    self.registration.showNotification(title,{
      body,
      icon:n.icon||d.icon||"./assets/logo.webp",
      badge:d.badge||"./assets/logo.webp",
      tag:d.tag||d.messageId||"fast-messenger-notification",
      renotify:true,
      data:{url:target,route:d.route||"",type:d.type||"general",messageId:d.messageId||"",senderUid:d.senderUid||""}
    });
  });
}catch(e){console.warn("FCM service worker initialization failed",e)}

self.addEventListener("push",e=>{
  if(isNativeWebView())e.stopImmediatePropagation();
});

function isStaticAsset(request){
  const u=new URL(request.url);
  return /\.(css|js|webp|png|jpg|jpeg|svg|woff2)$/i.test(u.pathname);
}

self.addEventListener("install",e=>e.waitUntil(caches.open(CACHE).then(c=>c.addAll(SHELL)).then(()=>self.skipWaiting())));
self.addEventListener("activate",e=>e.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>![CACHE,RUNTIME].includes(k)).map(k=>caches.delete(k)))).then(()=>self.clients.claim())));

self.addEventListener("notificationclick",event=>{
  event.notification.close();
  const d=event.notification?.data||{};
  const target=d.url||d.route||"./";
  event.waitUntil(clients.matchAll({type:"window",includeUncontrolled:true}).then(list=>{
    for(const client of list){
      if("focus" in client){
        client.postMessage({type:"FCM_NOTIFICATION_CLICK",route:d.route||"",url:target,notificationType:d.type||"general",messageId:d.messageId||"",senderUid:d.senderUid||""}).catch?.(()=>{});
        return client.focus();
      }
    }
    return clients.openWindow?clients.openWindow(target):undefined;
  }));
});

self.addEventListener("fetch",e=>{
  if(e.request.method!=="GET")return;
  const request=e.request,u=new URL(request.url);

  if(request.destination==="image"){
    e.respondWith(caches.open(RUNTIME).then(async cache=>{
      const cached=await cache.match(request);
      const network=fetch(request).then(r=>{if(r.ok||r.type==="opaque")cache.put(request,r.clone()).catch(()=>{});return r}).catch(()=>cached||Response.error());
      return cached||network;
    }));
    return;
  }

  if(u.origin!==location.origin)return;

  if(isStaticAsset(request)){
    e.respondWith(caches.open(RUNTIME).then(async cache=>{
      const cached=await cache.match(request);
      const network=fetch(request).then(r=>{if(r.ok)cache.put(request,r.clone()).catch(()=>{});return r}).catch(()=>cached);
      return cached||network;
    }));
    return;
  }

  const isHTML=u.pathname.endsWith("/")||u.pathname.endsWith("/index.html");
  if(isHTML){
    e.respondWith(fetch(request,{cache:"no-store"}).then(r=>{if(r.ok)caches.open(CACHE).then(c=>c.put(request,r.clone())).catch(()=>{});return r}).catch(()=>caches.match(request).then(r=>r||caches.match("./index.html"))));
    return;
  }

  e.respondWith(caches.match(request).then(cached=>{
    const network=fetch(request).then(r=>{if(r.ok)caches.open(RUNTIME).then(c=>c.put(request,r.clone())).catch(()=>{});return r}).catch(()=>cached||caches.match("./index.html"));
    return cached||network;
  }));
});
