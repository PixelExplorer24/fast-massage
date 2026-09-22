const firebaseConfig={
  apiKey:"AIzaSyDc-XZGguSKRxZUIG6s5h0sjhcuJrtZQZc",
  authDomain:"fast-massage-3ac80.firebaseapp.com",
  databaseURL:"https://fast-massage-3ac80-default-rtdb.firebaseio.com",
  projectId:"fast-massage-3ac80",
  storageBucket:"fast-massage-3ac80.firebasestorage.app",
  messagingSenderId:"114872273052",
  appId:"1:114872273052:web:f0873cc1f474b884ef4559",
  measurementId:"G-Y6YHPFTQQ8"
};

firebase.initializeApp(firebaseConfig);
const auth=firebase.auth(),db=firebase.database(),APP="fast-massage-3ac80";

// -----------------------------------------------------------------------------
// Firestore-shaped compatibility layer backed entirely by Firebase Realtime DB.
// The UI/business logic below can keep its existing collection/doc/query calls,
// while every read/write is actually performed through Realtime Database.
// -----------------------------------------------------------------------------
const RTDB_DELETE=Symbol("RTDB_DELETE"),RTDB_SERVER_TIMESTAMP=Symbol("RTDB_SERVER_TIMESTAMP");
const RTDB_ARRAY_UNION=Symbol("RTDB_ARRAY_UNION");
function rtdbNow(){return Date.now()}
function rtdbToMillis(v){
  if(v&&typeof v.toMillis==="function")return v.toMillis();
  if(v&&v.__rtdbTimestamp!=null)return Number(v.__rtdbTimestamp);
  return typeof v==="number"?v:Number(v)||0;
}
function rtdbResolve(v,oldValue){
  if(v===RTDB_SERVER_TIMESTAMP)return rtdbNow();
  if(v===RTDB_DELETE)return undefined;
  if(v&&v.__rtdbArrayUnion)return Array.from(new Set([...(Array.isArray(oldValue)?oldValue:[]),...v.values]));
  if(v&&v.__rtdbTimestamp!=null)return Number(v.__rtdbTimestamp);
  if(Array.isArray(v))return v.map((x,i)=>rtdbResolve(x,oldValue?.[i]));
  if(v&&typeof v==="object"){
    const out={};
    Object.entries(v).forEach(([k,x])=>{
      const y=rtdbResolve(x,oldValue?.[k]);
      if(y!==undefined)out[k]=y;
    });
    return out;
  }
  return v;
}
function mergeObject(base,patch){
  const out=base&&typeof base==="object"&&!Array.isArray(base)?{...base}:{};
  Object.entries(patch||{}).forEach(([k,v])=>{
    const y=rtdbResolve(v,out[k]);
    if(y===undefined)delete out[k];else out[k]=y;
  });
  return out;
}
class RTDocSnap{
  constructor(id,value){this.id=id;this._value=value==null?null:value;this.exists=this._value!==null&&this._value!==undefined}
  data(){return this.exists?this._value:undefined}
}
class RTQuerySnap{
  constructor(docs,changes=[]){this.docs=docs;this._changes=changes}
  docChanges(){return this._changes}
}
class RTDoc{
  constructor(path,id){this.path=path.replace(/^\/|\/$/g,"");this.id=id}
  get ref(){return this}
  child(name){return new RTDoc(`${this.path}/${name}`,name)}
  collection(name){return new RTCollection(`${this.path}/${name}`)}
  async get(){const snap=await db.ref(this.path).once("value");return new RTDocSnap(this.id,snap.val())}
  onSnapshot(cb,err){let first=true,previous=null;const r=db.ref(this.path);const fn=s=>{try{const val=s.val();const snap=new RTDocSnap(this.id,val);if(first){previous=val;first=false}else previous=val;cb(snap)}catch(e){err?.(e)}};r.on("value",fn,e=>err?.(e));return()=>r.off("value",fn)}
  async set(data,opts={}){
    const ref=db.ref(this.path);
    if(opts?.merge){const old=(await ref.once("value")).val()||{};return ref.set(mergeObject(old,data))}
    return ref.set(rtdbResolve(data,null));
  }
  async update(data){const old=(await db.ref(this.path).once("value")).val()||{};return db.ref(this.path).set(mergeObject(old,data))}
  async delete(){return db.ref(this.path).remove()}
}
class RTCollection{
  constructor(path){this.path=path.replace(/^\/|\/$/g,"");this.filters=[]}
  doc(id){const key=id||db.ref(this.path).push().key;return new RTDoc(`${this.path}/${key}`,key)}
  add(data){const key=db.ref(this.path).push().key;const ref=new RTDoc(`${this.path}/${key}`,key);return ref.set(data).then(()=>ref)}
  where(field,op,value){const q=new RTQuery(this.path,this.filters);return q.where(field,op,value)}
  async get(){return new RTQuery(this.path,[]).get()}
  onSnapshot(cb,err){return new RTQuery(this.path,[]).onSnapshot(cb,err)}
}
class RTQuery{
  constructor(path,filters=[]){this.path=path;this.filters=[...filters]}
  where(field,op,value){return new RTQuery(this.path,[...this.filters,{field,op,value}])}
  _match(obj){return this.filters.every(f=>{const a=obj?.[f.field],b=f.value;switch(f.op){case "==":return a===b;case ">":return rtdbToMillis(a)>rtdbToMillis(b);case ">=":return rtdbToMillis(a)>=rtdbToMillis(b);case "<":return rtdbToMillis(a)<rtdbToMillis(b);case "<=":return rtdbToMillis(a)<=rtdbToMillis(b);case "array-contains":return Array.isArray(a)&&a.some(x=>String(x)===String(b));default:return false}})}
  async _read(){const snap=await db.ref(this.path).once("value");const raw=snap.val()||{};return Object.entries(raw).filter(([,v])=>v&&this._match(v)).map(([id,v])=>new RTDocSnap(id,v))}
  async get(){return new RTQuerySnap(await this._read())}
  onSnapshot(cb,err){let previous=new Map(),first=true;const r=db.ref(this.path);const fn=async snap=>{try{const raw=snap.val()||{};const current=new Map(Object.entries(raw).filter(([,v])=>v&&this._match(v)));const changes=[];for(const [id,v] of current){if(!previous.has(id))changes.push({type:"added",doc:new RTDocSnap(id,v)});else if(JSON.stringify(previous.get(id))!==JSON.stringify(v))changes.push({type:"modified",doc:new RTDocSnap(id,v)})}for(const [id,v] of previous)if(!current.has(id))changes.push({type:"removed",doc:new RTDocSnap(id,v)});const docs=[...current.entries()].map(([id,v])=>new RTDocSnap(id,v));previous=current;cb(new RTQuerySnap(docs,first?docs.map(d=>({type:"added",doc:d})):changes));first=false}catch(e){err?.(e)}};r.on("value",fn,e=>err?.(e));return()=>r.off("value",fn)}
}
class RTBatch{
  constructor(){this.ops=[]}
  set(ref,data,opts){this.ops.push(()=>ref.set(data,opts));return this}
  update(ref,data){this.ops.push(()=>ref.update(data));return this}
  delete(ref){this.ops.push(()=>ref.delete());return this}
  async commit(){for(const op of this.ops)await op()}
}
// Minimal firebase.firestore namespace retained only as a compatibility API.
firebase.firestore={
  FieldValue:{serverTimestamp:()=>RTDB_SERVER_TIMESTAMP,delete:()=>RTDB_DELETE,arrayUnion:(...values)=>({__rtdbArrayUnion:true,values})},
  Timestamp:{now:()=>({__rtdbTimestamp:Date.now(),toMillis(){return this.__rtdbTimestamp}}),fromMillis:(n)=>({__rtdbTimestamp:Number(n)||0,toMillis(){return this.__rtdbTimestamp}})}
};
db.batch=()=>new RTBatch();
db.enablePersistence=()=>Promise.resolve();
const ROOT=()=>new RTCollection("");
const USERS=()=>new RTCollection("users"),FRIENDS=()=>new RTCollection("friends"),REQUESTS=()=>new RTCollection("friendRequests"),MESSAGES=()=>new RTCollection("messages"),GROUPS=()=>new RTCollection("groups");
const CALLS=()=>new RTCollection("calls");

const IMAGE_UPLOAD_KEY="1abc9f66636c45ace1d0952e080d153d";
const FILE_UPLOAD_ENDPOINT="https://upload.gofile.io/uploadfile";
let me=null,profile=null,users=[],friends=[],requests=[],sentRequests=[],groups=[],activeFriend=null,chatUnsubs=[],listUnsubs=[],typingUnsub=null,typingTimer=null,attachedImages=[],attachedFiles=[],messageMap=new Map(),activeMessageMap=new Map(),peopleTab="friends";
const CACHE_PREFIX="fm_cache_v12_";
const PERSISTENT_FRIENDS_PREFIX="fm_friend_registry_v1_";
const PERSISTENT_ROOMS_PREFIX="fm_chat_rooms_registry_v1_";
let friendsSyncReady=false,groupsSyncReady=false,messagesSyncReady=false;
let authResolved=false;
const AGORA_APP_ID="addaf4af54e845beb818de869a7de813";
let agoraClient=null,localMicTrack=null,localCamTrack=null,activeCall=null,incomingCall=null,callUnsub=null,callInviteUnsub=null,notificationUnsub=null,remoteUsers=new Map();
let callTimerInterval=null,callStartedAt=0,callRingTimer=null;
const callEventLocks=new Set();
function callMillis(v){return v?.toMillis?v.toMillis():rtdbToMillis(v)}
function callEventDuration(c){
  const start=callMillis(c?.acceptedAt);
  const end=callMillis(c?.endedAt)||Date.now();
  return start&&end>start?Math.max(0,end-start):0;
}
function callDurationText(ms){
  const total=Math.max(0,Math.round(Number(ms||0)/1000));
  const h=Math.floor(total/3600),m=Math.floor((total%3600)/60),s=total%60;
  return h?`${h}h ${String(m).padStart(2,"0")}m`:m?`${m}m ${String(s).padStart(2,"0")}s`:`${s}s`;
}
function callEventLabel(m){
  const video=m.callMode==="video", type=video?"ভিডিও কল":"অডিও কল";
  const mine=String(m.callCallerUid||m.callerUid||m.senderUid)===String(me?.uid);
  const outcome=m.callOutcome||"missed";
  if(outcome==="completed"){
    return `<i class="fa-solid ${video?"fa-video":"fa-phone"}"></i><span>${mine?"Outgoing":"Incoming"} ${type}<small>${esc(callDurationText(m.callDurationMs||0))}</small></span>`;
  }
  if(mine)return `<i class="fa-solid ${video?"fa-video-slash":"fa-phone-slash"}"></i><span>${outcome==="rejected"?`${type} declined`:`${type} · No answer`}<small>Missed call</small></span>`;
  return `<i class="fa-solid ${video?"fa-video-slash":"fa-phone-slash"}"></i><span>Missed ${type}<small>${outcome==="rejected"?"Call declined":"No answer"}</small></span>`;
}
async function saveCallToChat(c,forcedStatus){
  if(!c?.callId||!me)return;
  const key=String(c.callId);
  if(callEventLocks.has(key))return;
  callEventLocks.add(key);
  try{
    const status=forcedStatus||c.status||"ended";
    const durationMs=status==="accepted"||c.acceptedAt?callEventDuration(c):0;
    const outcome=(status==="ended"&&durationMs>0)?"completed":(status==="rejected"?"rejected":"missed");
    const participants=Array.isArray(c.recipientUids)?c.recipientUids:[];
    const otherUid=c.groupId?null:(participants.find(x=>String(x)!==String(me.uid))||c.receiverUid||c.callerUid);
    const memberUids=c.memberUids||participants.concat([c.callerUid]).filter(Boolean);
    const ref=MESSAGES().doc(`call_${key}`);
    const payload={
      senderUid:me.uid,
      receiverUid:c.groupId?null:otherUid,
      groupId:c.groupId||null,
      groupMemberUids:c.groupId?memberUids:null,
      groupMemberMap:c.groupId?Object.fromEntries(memberUids.map(x=>[String(x),true])):null,
      text:"",imageUrls:[],files:[],createdAt:firebase.firestore.FieldValue.serverTimestamp(),seen:false,
      type:"call",callId:key,callCallerUid:c.callerUid,callCallerName:c.callerName||"User",callCallerPhoto:c.callerPhoto||null,
      callMode:c.mode||"audio",callOutcome:outcome,callDurationMs:durationMs,callEndedAt:c.endedAt||Date.now()
    };
    await ref.set(payload,{merge:true});
  }catch(e){console.warn("saveCallToChat",e)}finally{callEventLocks.delete(key)}
}
const CALL_TOKEN=null; // Keep null when Agora App Certificate/token authentication is disabled.
function callChannel(id){return "fm_"+String(id).replace(/[^a-zA-Z0-9_-]/g,"").slice(0,55)}
function callTarget(){return activeFriend?.isGroup?activeFriend.uid:activeFriend?.uid}
function participantName(uid){if(String(uid)===String(me?.uid))return "You";const u=users.find(x=>String(x.uid)===String(uid));return u?.displayName||u?.email?.split("@")[0]||"Participant"}
function setCallStatus(t){if($("callStatus"))$("callStatus").textContent=t}
function formatCallDuration(ms){const total=Math.max(0,Math.floor(ms/1000));const h=Math.floor(total/3600),m=Math.floor((total%3600)/60),s=total%60;return h?`${String(h).padStart(2,"0")}:${String(m).padStart(2,"0")}:${String(s).padStart(2,"0")}`:`${String(m).padStart(2,"0")}:${String(s).padStart(2,"0")}`}
function stopCallTimer(){
  if(callTimerInterval){clearInterval(callTimerInterval);callTimerInterval=null}
  if(callRingTimer){clearTimeout(callRingTimer);callRingTimer=null}
  callStartedAt=0;
  const e=$("callTimer");if(e){e.textContent="00:00";e.classList.add("hidden")}
}
function startCallTimer(startMs){stopCallTimer();callStartedAt=Number(startMs)||Date.now();const e=$("callTimer");if(!e)return;e.classList.remove("hidden");const tick=()=>{if(e) e.textContent=formatCallDuration(Date.now()-callStartedAt)};tick();callTimerInterval=setInterval(tick,1000)}
function setCallNetwork(level){const e=$("callNetwork");if(!e)return;e.className="call-network "+(level==="poor"?"bad":level==="fair"?"ok":"");e.innerHTML=`<i class="fa-solid fa-signal"></i> ${level==="poor"?"Weak":level==="fair"?"Fair":"Good"}`}
let callTransitionTimer=null;
function callUi(show){
  const overlay=$("callOverlay");
  if(!overlay)return;
  clearTimeout(callTransitionTimer);
  $("callParticipantsPanel")?.classList.add("hidden");
  if(show){
    overlay.classList.remove("hidden","call-exiting");
    // Force a fresh animation even when switching rapidly between calls.
    overlay.classList.remove("call-entering"); void overlay.offsetWidth; overlay.classList.add("call-entering");
    if($("callEmpty"))$("callEmpty").classList.remove("hidden");
    callTransitionTimer=setTimeout(()=>overlay.classList.remove("call-entering"),700);
  }else{
    overlay.classList.remove("call-entering");
    if(!overlay.classList.contains("hidden")){
      overlay.classList.add("call-exiting");
      callTransitionTimer=setTimeout(()=>{overlay.classList.add("hidden");overlay.classList.remove("call-exiting");},360);
    }else overlay.classList.add("hidden");
  }
}
function addRemoteVideo(user){
  const id="remote_"+user.uid; let box=$(id);
  if(!box){box=document.createElement("div");box.id=id;box.className="remote-video";box.innerHTML=`<div id="${id}_view"></div><div class="remote-label-wrap"><span class="remote-label-name">${esc(participantName(user.uid))}</span><span class="remote-label-status">Live</span></div>`;$("remoteVideos").appendChild(box)}
  $("callEmpty")?.classList.add("hidden"); user.videoTrack?.play(id+"_view"); updateCallParticipants();
}
function removeRemoteVideo(uid){$("remote_"+uid)?.remove();remoteUsers.delete(uid);if(!$("remoteVideos")?.children.length)$("callEmpty")?.classList.remove("hidden");updateCallParticipants()}
function cleanupCallUI(){stopCallTimer();remoteUsers.forEach((u)=>{try{u.videoTrack?.stop()}catch(_){}});remoteUsers.clear();$("remoteVideos").innerHTML="";$("localVideo").innerHTML="";$("localVideoWrap").classList.add("hidden");$("callEmpty").classList.remove("hidden");callUi(false)}
function updateCallParticipants(){
  const header=$("callHeaderName");
  if(!header)return;
  const group=activeCall?.groupId ? groups.find(g=>g.uid===activeCall.groupId||g.id===activeCall.groupId) : null;
  if(activeCall?.groupId){
    const connected=remoteUsers.size+1;
    header.textContent=(group?.name||"Group call") + ` · ${connected} participants`;
  }
  document.querySelectorAll(".remote-video").forEach(el=>{
    const uid=el.id.replace(/^remote_/,'');
    const n=el.querySelector(".remote-label-name");
    if(n)n.textContent=participantName(uid);
    el.classList.toggle("pinned",String(uid)===String(pinnedCallParticipant));
  });
  const groupBtn=$("callParticipantsBtn");
  if(groupBtn){
    groupBtn.classList.toggle("hidden",!activeCall?.groupId);
    const badge=$("callParticipantBadge");
    if(badge)badge.textContent=String(remoteUsers.size+1);
  }
  if(activeCall?.groupId)renderCallParticipants();
}
function callParticipantRows(){
  const rows=[{uid:me?.uid,name:"You",photo:profile?.photoURL||me?.photoURL||null,self:true}];
  remoteUsers.forEach(u=>rows.push({uid:String(u.uid),name:participantName(u.uid),photo:users.find(x=>String(x.uid)===String(u.uid))?.photoURL||null,user:u}));
  return rows;
}
function renderCallParticipants(){
  const list=$("callParticipantsList"),sub=$("callParticipantsSub");
  if(!list)return;
  const rows=callParticipantRows();
  if(sub)sub.textContent=`${rows.length} connected`;
  list.innerHTML="";
  rows.forEach(p=>{
    const row=document.createElement("div");row.className="call-participant-row";
    const muted=!p.self&&mutedRemoteParticipants.has(String(p.uid));
    const pinned=String(p.uid)===String(pinnedCallParticipant);
    row.innerHTML=`<img class="call-participant-avatar" src="${esc(p.photo||avatar(users.find(u=>String(u.uid)===String(p.uid))))}" alt="">
      <div class="call-participant-info"><b>${esc(p.name)}${p.self?" (You)":""}</b><small>${p.self?"Your microphone":"Connected"}${p.uid===pinnedCallParticipant?" · Pinned":""}</small></div>
      <div class="call-participant-actions">
        ${!p.self?`<button class="call-participant-action ${muted?"active":""}" data-call-person-action="mute" data-uid="${esc(p.uid)}" title="${muted?"Unmute":"Mute"}"><i class="fa-solid ${muted?"fa-volume-xmark":"fa-volume-high"}"></i></button>`:""}
        ${!p.self?`<button class="call-participant-action ${pinned?"active":""}" data-call-person-action="pin" data-uid="${esc(p.uid)}" title="${pinned?"Unpin":"Pin"}"><i class="fa-solid fa-thumbtack"></i></button>`:""}
        ${!p.self&&activeCall?.caller?`<button class="call-participant-action danger" data-call-person-action="remove" data-uid="${esc(p.uid)}" title="Remove"><i class="fa-solid fa-user-minus"></i></button>`:""}
      </div>`;
    list.appendChild(row);
  });
  if(!rows.length)list.innerHTML='<div class="call-participant-empty">No connected participants.</div>';
}
async function toggleRemoteMute(uid){
  const key=String(uid),u=remoteUsers.get(uid)||remoteUsers.get(Number(uid));
  if(!u?.audioTrack)return;
  const next=!mutedRemoteParticipants.has(key);
  try{u.audioTrack.setVolume(next?0:100)}catch(_){}
  next?mutedRemoteParticipants.add(key):mutedRemoteParticipants.delete(key);
  renderCallParticipants();
}
function pinCallParticipant(uid){
  pinnedCallParticipant=String(pinnedCallParticipant)===String(uid)?null:String(uid);
  document.querySelectorAll(".remote-video").forEach(el=>el.classList.toggle("pinned",el.id.replace(/^remote_/,'')===pinnedCallParticipant));
  renderCallParticipants();
}
async function removeCallParticipant(uid){
  if(!activeCall?.caller||!activeCall?.ref)return;
  if(!confirm("এই participant-কে call থেকে remove করবেন?"))return;
  const key=String(uid);
  try{
    await activeCall.ref.set({kickedUids:firebase.firestore.FieldValue.arrayUnion(key)},{merge:true});
    const u=remoteUsers.get(uid)||remoteUsers.get(Number(uid));
    try{u?.audioTrack?.stop()}catch(_){}
    removeRemoteVideo(uid);remoteUsers.delete(uid);updateCallParticipants();
    toast("Participant removed");
  }catch(e){console.error(e);toast("Participant remove করা যায়নি")}
}
function closeCallParticipants(){ $("callParticipantsPanel")?.classList.add("hidden"); }
async function setupAgora(mode,channel){
  if(!window.AgoraRTC)throw new Error("Agora SDK load হয়নি");
  if(!navigator.mediaDevices?.getUserMedia)throw new Error("এই ব্রাউজারে microphone/camera access নেই");
  if(agoraClient){try{agoraClient.removeAllListeners();await agoraClient.leave()}catch(_){} agoraClient=null}

  // Ask for the required permission before joining Agora. This avoids the common
  // case where the call UI opens but the microphone track never becomes active.
  const permissionStream=await navigator.mediaDevices.getUserMedia(mode==="video"?{audio:true,video:true}:{audio:true,video:false});
  permissionStream.getTracks().forEach(t=>t.stop());

  agoraClient=AgoraRTC.createClient({mode:"rtc",codec:"vp8"});
  agoraClient.on("user-published",async(user,mediaType)=>{
    try{
      await agoraClient.subscribe(user,mediaType);
      remoteUsers.set(user.uid,user);
      if(mediaType==="video")addRemoteVideo(user);
      if(mediaType==="audio"){
        if(user.audioTrack){
          await user.audioTrack.play().catch(err=>console.warn("Remote audio autoplay:",err));
          await applyPlaybackDevice(user.audioTrack);
        }
      }
      updateCallParticipants();
    }catch(err){console.error("Agora subscribe:",err);toast("অন্য পক্ষের অডিও/ভিডিও সংযোগ করা যায়নি")}
  });
  agoraClient.on("user-unpublished",(user,mediaType)=>{
    if(mediaType==="video")removeRemoteVideo(user.uid);
    if(mediaType==="audio")updateCallParticipants();
  });
  agoraClient.on("user-left",user=>{removeRemoteVideo(user.uid);remoteUsers.delete(user.uid);updateCallParticipants()});
  agoraClient.on("network-quality",q=>{const n=Math.max(q.uplinkNetworkQuality||0,q.downlinkNetworkQuality||0);setCallNetwork(n>=5?"poor":n>=3?"fair":"good")});
  agoraClient.on("connection-state-change",(cur,prev,reason)=>{
    if(cur==="DISCONNECTED"||cur==="FAILED")setCallStatus("কল সংযোগ বিচ্ছিন্ন…");
    if(cur==="CONNECTED"&&activeCall?.lastData?.status!=="accepted")setCallStatus("কল সংযোগ হয়েছে…");
  });

  // If Agora App Certificate is enabled in the Agora project, CALL_TOKEN must
  // be replaced by a valid server-issued token. With certificate disabled, null
  // is correct for this client-only setup.
  await agoraClient.join(AGORA_APP_ID,channel,CALL_TOKEN,me?.uid||null);
  if(mode==="audio"){
    localMicTrack=await AgoraRTC.createMicrophoneAudioTrack({encoderConfig:"speech_low_quality"});
    await localMicTrack.setMuted(false);
  }else{
    [localMicTrack,localCamTrack]=await AgoraRTC.createMicrophoneAndCameraTracks(
      {encoderConfig:"speech_low_quality"},
      {encoderConfig:{width:1920,height:1080,frameRate:30,bitrateMin:800,bitrateMax:4500}}
    );
    await localMicTrack.setMuted(false);
    $("localVideoWrap").classList.remove("hidden");localCamTrack.play("localVideo");
  }
  await agoraClient.publish(mode==="audio"?[localMicTrack]:[localMicTrack,localCamTrack]);
}
let cameraPreviewStream=null,cameraPreviewFacing="user",cameraPreviewMuted=false,cameraPreviewLightOn=false,pendingVideoCall=null;
function closeCameraPreview(){
  if(cameraPreviewStream){cameraPreviewStream.getTracks().forEach(t=>t.stop());cameraPreviewStream=null}
  $("cameraPreviewVideo")?.pause();$("cameraPreviewVideo")?.removeAttribute("srcObject");$("cameraPreviewModal")?.classList.add("hidden");pendingVideoCall=null;
}
async function openCameraPreview(){
  const modal=$("cameraPreviewModal"),video=$("cameraPreviewVideo"); if(!modal||!video)return false;
  try{
    cameraPreviewFacing="user"; cameraPreviewMuted=false; cameraPreviewLightOn=false;
    cameraPreviewStream=await navigator.mediaDevices.getUserMedia({video:{facingMode:{ideal:"user"},width:{ideal:1920},height:{ideal:1080}},audio:true});
    video.srcObject=cameraPreviewStream; await video.play().catch(()=>{});
    $("previewMuteBtn")?.classList.add("active"); $("previewMuteBtn")?.classList.remove("muted");
    $("cameraPreviewLight")?.classList.add("hidden"); $("cameraPreviewStatus").innerHTML='<i class="fa-solid fa-circle"></i> Camera ready';
    modal.classList.remove("hidden"); return true;
  }catch(e){console.error(e);toast("Camera permission is required for video call");return false}
}
async function flipPreviewCamera(){
  if(!cameraPreviewStream)return;
  const wasMuted=cameraPreviewMuted;
  cameraPreviewStream.getTracks().forEach(t=>t.stop());
  cameraPreviewFacing=cameraPreviewFacing==="user"?"environment":"user";
  try{cameraPreviewStream=await navigator.mediaDevices.getUserMedia({video:{facingMode:{ideal:cameraPreviewFacing},width:{ideal:1920},height:{ideal:1080}},audio:true});
    cameraPreviewStream.getAudioTracks().forEach(t=>t.enabled=!wasMuted);$("cameraPreviewVideo").srcObject=cameraPreviewStream;await $("cameraPreviewVideo").play().catch(()=>{});$("cameraPreviewVideo").style.transform=cameraPreviewFacing==="user"?"scaleX(-1)":"scaleX(1)";}catch(e){toast("Unable to switch camera")}
}
function togglePreviewMute(){cameraPreviewMuted=!cameraPreviewMuted;cameraPreviewStream?.getAudioTracks().forEach(t=>t.enabled=!cameraPreviewMuted);const b=$("previewMuteBtn");b?.classList.toggle("active",!cameraPreviewMuted);b?.classList.toggle("muted",cameraPreviewMuted);if(b)b.innerHTML=cameraPreviewMuted?'<i class="fa-solid fa-microphone-slash"></i><span>Muted</span>':'<i class="fa-solid fa-microphone"></i><span>Mic</span>'}
function togglePreviewLight(){cameraPreviewLightOn=!cameraPreviewLightOn;$("cameraPreviewLight")?.classList.toggle("hidden",!cameraPreviewLightOn);$("previewLightBtn")?.classList.toggle("active",cameraPreviewLightOn)}

function callSnapshotFallback(snapshot,payload){return {...payload,...(snapshot||{})};}
async function launchCall(mode){
  if(!me||!activeFriend)return;
  if(!activeFriend.isGroup&&!isFriend(activeFriend.uid))return toast("আগে Friend Request গ্রহণ করতে হবে");
  const channel=callChannel(activeFriend.isGroup?activeFriend.uid:pair(me.uid,activeFriend.uid));
  const callId=CALLS().doc().id;
  const participants=activeFriend.isGroup?(activeFriend.memberUids||[]).filter(x=>x!==me.uid):[activeFriend.uid];
  const ref=CALLS().doc(callId);
  const payload={callId,callerUid:me.uid,callerName:profile?.displayName||me.displayName||"User",callerPhoto:profile?.photoURL||me.photoURL||null,mode,channel,groupId:activeFriend.isGroup?activeFriend.uid:null,memberUids:activeFriend.isGroup?(activeFriend.memberUids||[]):[],recipientUids:participants,recipientMap:Object.fromEntries(participants.map(x=>[String(x),true])),status:"ringing",createdAt:firebase.firestore.FieldValue.serverTimestamp()};
  await ref.set(payload);
  activeCall={callId,mode,channel,ref,caller:true,groupId:activeFriend.isGroup?activeFriend.uid:null};watchActiveCall();
  $("callHeaderName").textContent=activeFriend.isGroup?`${activeFriend.name||"Group"} · Group call`:activeFriend.displayName||"Call";
  $("callHeaderAvatar").src=avatar(activeFriend);callUi(true);updateCallParticipants();setCallStatus("Connecting…");
  try{
    await setupAgora(mode,channel);
    setCallStatus("কলের উত্তর অপেক্ষা…");
    // No-answer calls automatically become missed calls instead of remaining
    // in the ringing state forever.
    if(callRingTimer)clearTimeout(callRingTimer);
    callRingTimer=setTimeout(async()=>{
      if(activeCall?.callId!==callId||!activeCall?.caller)return;
      try{
        const endedAt=Date.now();
        await ref.set({status:"ended",endedBy:me.uid,endedAt,callOutcome:"missed"},{merge:true});
        const snap=await ref.get();
        await saveCallToChat({...callSnapshotFallback(snap.data(),payload),status:"ended",endedAt},"ended");
      }catch(err){console.warn("missed call timeout",err)}
      await endCall(true);
    },30000);
  }
  catch(e){console.error("launchCall:",e);toast(e?.message?.includes("permission")||e?.name==="NotAllowedError"?"Microphone permission দিন":"কল শুরু করা যায়নি");await endCall(false)}

}
async function startCall(mode){
  if(mode==="video"){
    if(!me||!activeFriend)return;
    if(!activeFriend.isGroup&&!isFriend(activeFriend.uid))return toast("আগে Friend Request গ্রহণ করতে হবে");
    pendingVideoCall={mode};
    $("cameraPreviewTargetName").textContent=activeFriend.isGroup?(activeFriend.name||"Group call"):(activeFriend.displayName||"Video call");
    if(await openCameraPreview())return;
    pendingVideoCall=null; return;
  }
  return launchCall(mode);
}

async function acceptCall(){
  const c=incomingCall;if(!c)return;
  if(activeCall){toast("আপনি ইতিমধ্যে একটি কলে আছেন");return;}
  $("callInviteModal").classList.add("hidden");incomingCall=null;
  activeCall={callId:c.callId,mode:c.mode,channel:c.channel,ref:CALLS().doc(c.callId),caller:false,groupId:c.groupId||null};watchActiveCall();
  $("callHeaderName").textContent=c.groupId?(c.callerName+" · Group call"):c.callerName;$("callHeaderAvatar").src=c.callerPhoto||avatar(users.find(u=>u.uid===c.callerUid));callUi(true);updateCallParticipants();setCallStatus("Connecting…");
  try{await setupAgora(c.mode,c.channel);setCallStatus(c.mode==="video"?"ভিডিও কল চলছে":"অডিও কল চলছে");startCallTimer(Date.now());await activeCall.ref.set({status:"accepted",acceptedBy:me.uid,acceptedAt:firebase.firestore.FieldValue.serverTimestamp()},{merge:true})}
  catch(e){console.error("acceptCall:",e);toast(e?.message?.includes("permission")||e?.name==="NotAllowedError"?"Microphone/Camera permission দিন":"কল গ্রহণ করা যায়নি");await endCall(false)}
}
async function rejectIncomingCall(){const c=incomingCall;if(!c)return;$("callInviteModal").classList.add("hidden");incomingCall=null;try{const endedAt=Date.now();await CALLS().doc(c.callId).set({status:"rejected",rejectedBy:me.uid,rejectedAt:firebase.firestore.FieldValue.serverTimestamp(),endedAt},{merge:true});await saveCallToChat({...c,status:"rejected",endedAt},"rejected")}catch(_){}}
async function endCall(silent=false){
  // Close the local call UI immediately. Do not wait for Firebase/RTDB or Agora
  // cleanup before hiding the overlay; otherwise the End button can appear dead
  // on a slow connection.
  const c=activeCall;
  activeCall=null;
  if(callUnsub){try{callUnsub()}catch(_){} callUnsub=null;}
  try{localMicTrack?.stop();localMicTrack?.close();localCamTrack?.stop();localCamTrack?.close()}catch(_){ }
  localMicTrack=localCamTrack=null;
  const client=agoraClient;
  agoraClient=null;
  if(client){try{client.removeAllListeners();await client.leave()}catch(e){console.warn("Agora leave:",e)} }
  pinnedCallParticipant=null;mutedRemoteParticipants.clear();closeCallParticipants();cleanupCallUI();incomingCall=null;

  if(c&&!silent){
    const endedAt=Date.now();
    // Persist the end event without blocking the UI.
    (async()=>{
      try{
        const snap=await c.ref.get();
        const current={...(snap.data()||{}),...c};
        await c.ref.set({status:"ended",endedBy:me.uid,endedAt},{merge:true});
        await saveCallToChat({...current,status:"ended",endedAt},"ended");
      }catch(e){console.warn("endCall",e)}
    })();
  }
}


let selectedPlaybackDevice="default";
let playbackMode="speaker";
async function listPlaybackDevices(){
  try{
    if(!window.AgoraRTC?.getPlaybackDevices)return [];
    return await AgoraRTC.getPlaybackDevices();
  }catch(e){console.warn("playback devices",e);return []}
}
async function applyPlaybackDevice(track){
  if(!track)return;
  try{
    if(typeof track.setPlaybackDevice==="function" && selectedPlaybackDevice!=="default"){await track.setPlaybackDevice(selectedPlaybackDevice);return true}
  }catch(e){console.warn("set playback device",e)}
  return false
}
async function setPlaybackOutput(deviceId,mode="speaker"){
  playbackMode=mode;selectedPlaybackDevice=deviceId||"default";
  const tracks=[];remoteUsers.forEach(u=>{if(u.audioTrack)tracks.push(u.audioTrack)});
  for(const track of tracks){try{await applyPlaybackDevice(track)}catch(_){}}
  const btn=$("speakerCallBtn");
  if(btn){btn.classList.add("active");btn.innerHTML=mode==="earpiece"?'<i class="fa-solid fa-mobile-screen-button"></i><span>Earpiece</span>':'<i class="fa-solid fa-volume-high"></i><span>Speaker</span>'}
  $("audioOutputMenu")?.classList.add("hidden");
  toast(mode==="earpiece"?"Earpiece selected":"Speaker selected");
}
async function openAudioOutputMenu(){
  const menu=$("audioOutputMenu");if(!menu)return;
  menu.classList.toggle("hidden");
  if(menu.classList.contains("hidden"))return;
  const list=$("audioOutputDevices");
  list.innerHTML='<div class="audio-output-loading">অডিও ডিভাইস খোঁজা হচ্ছে…</div>';
  const devices=await listPlaybackDevices();
  list.innerHTML="";
  const add=(label,icon,id,mode,disabled=false)=>{const b=document.createElement("button");b.className="audio-output-item"+(playbackMode===mode?" selected":"")+(disabled?" disabled":"");b.disabled=disabled;b.innerHTML=`<i class="fa-solid ${icon}"></i><span>${label}</span>${playbackMode===mode?'<i class="fa-solid fa-check check"></i>':''}`;b.onclick=()=>setPlaybackOutput(id,mode);list.appendChild(b)};
  add("Speaker","fa-volume-high","default","speaker");
  const receiver=devices.find(d=>/earpiece|receiver|handset|receiver|telephony/i.test(d.label||""));
  if(receiver)add("Earpiece / Receiver","fa-mobile-screen-button",receiver.deviceId,"earpiece");
  const extras=devices.filter(d=>d.deviceId!=="default" && (!receiver||d.deviceId!==receiver.deviceId));
  extras.slice(0,6).forEach(d=>add(d.label||"Audio output","fa-headphones",d.deviceId,"speaker"));
  if(!devices.length){const note=document.createElement("div");note.className="audio-output-note";note.textContent="এই ব্রাউজারে আলাদা output device শনাক্ত করা যায়নি। Speaker mode ব্যবহার করা হবে।";list.appendChild(note)}
}

async function toggleMute(){if(!localMicTrack)return;const muted=localMicTrack.muted;await localMicTrack.setMuted(!muted);$("muteCallBtn").classList.toggle("active",muted);$("muteCallBtn").innerHTML=muted?'<i class="fa-solid fa-microphone-slash"></i><span>Unmute</span>':'<i class="fa-solid fa-microphone"></i><span>Mute</span>'}
async function toggleCamera(){if(!localCamTrack)return;const muted=localCamTrack.muted;await localCamTrack.setMuted(!muted);$("cameraCallBtn").classList.toggle("active",muted);$("cameraCallBtn").innerHTML=muted?'<i class="fa-solid fa-video-slash"></i><span>Camera off</span>':'<i class="fa-solid fa-video"></i><span>Camera</span>'}
function watchCallInvites(){
  if(!me)return;
  if(callInviteUnsub)callInviteUnsub();
  callInviteUnsub=CALLS().where("recipientUids","array-contains",me.uid).onSnapshot(s=>{
    s.docChanges().filter(c=>c.type==="added"||c.type==="modified").forEach(ch=>{
      const c={id:ch.doc.id,...ch.doc.data()};
      if(c.callerUid===me.uid || c.status!=="ringing")return;
      if(activeCall || incomingCall?.callId===c.callId)return;
      incomingCall=c;$("incomingCallAvatar").src=c.callerPhoto||avatar(users.find(u=>u.uid===c.callerUid));$("incomingCallName").textContent=c.callerName||"Incoming call";$("incomingCallType").textContent=c.mode==="video"?"ভিডিও কল":"অডিও কল";$("callInviteModal").classList.remove("hidden");
    });
  },e=>console.warn("call invite listener",e));
}
function watchActiveCall(){
  if(callUnsub)callUnsub();
  if(!activeCall)return;
  callUnsub=activeCall.ref.onSnapshot(s=>{
    if(!s.exists)return;const c=s.data();
    activeCall.lastData=c;
    if(c.status==="accepted"&&!callStartedAt){setCallStatus(c.mode==="video"?"ভিডিও কল চলছে":"অডিও কল চলছে");startCallTimer(callMillis(c.acceptedAt)||Date.now())}
    if(c.kickedUids?.map(String).includes(String(me?.uid))){toast("আপনাকে group call থেকে remove করা হয়েছে");endCall(true);return;}
    if(c.status==="ended"||c.status==="rejected"){
      // Both sides write the same deterministic chat message, so the call
      // history appears in the same conversation even when the other person
      // ended/rejected the call.
      saveCallToChat({...c,callId:c.callId||activeCall.callId},c.status).catch(()=>{});
      endCall(true);
    }
  });
}

const $=id=>document.getElementById(id),esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;"}[c]));
const avatar=u=>u?.photoURL||"https://placehold.co/120x120/e5e7eb/64748b?text=U";
const pair=(a,b)=>[a,b].sort().join("__");
const time=v=>{let n=v?.toMillis?v.toMillis():Number(v||0);if(!n)return"now";let d=new Date(n),now=new Date();if(d.toDateString()===now.toDateString())return d.toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"});return d.toLocaleDateString([],{day:"2-digit",month:"short"});};
const bytes=n=>{if(!n)return"0 B";const u=["B","KB","MB","GB"];let i=Math.floor(Math.log(n)/Math.log(1024));return`${(n/Math.pow(1024,i)).toFixed(i?1:0)} ${u[i]}`};
function toast(t){const e=$("toast");e.textContent=t;e.classList.add("show");clearTimeout(toast.t);toast.t=setTimeout(()=>e.classList.remove("show"),2400)}
function isFriend(uid){return friends.some(f=>f.friendUid===uid)}
function closeAllModals(){document.querySelectorAll(".modal").forEach(x=>x.classList.add("hidden"))}
function installCallMessageStyles(){
  if($("callMessageStyles"))return;
  const st=document.createElement("style");st.id="callMessageStyles";st.textContent=`
    .call-bubble{min-width:190px;padding:11px 13px!important}
    .call-event{display:flex;align-items:center;gap:10px;font-weight:800;font-size:12px}
    .call-event>i{width:34px;height:34px;border-radius:11px;display:grid;place-items:center;background:#eef2ff;color:#4f46e5;font-size:14px}
    .call-event>span{display:block;min-width:0}
    .call-event small{display:block;font-size:9px;font-weight:600;opacity:.7;margin-top:3px}
    .mine .call-event>i{background:#ffffff24;color:#fff}
    .call-bubble.missed .call-event>i{background:#fef2f2;color:#dc2626}
    .mine .call-bubble.missed .call-event>i{background:#ffffff24;color:#fecaca}
    .call-row .msg-time{margin-top:7px}
  `;document.head.appendChild(st);
}
installCallMessageStyles();

function installChatRoomThemeStyles(){
  if($('chatRoomThemeStyles'))return;
  const st=document.createElement('style');st.id='chatRoomThemeStyles';st.textContent=`
    #chatPanel{position:fixed;inset:0;isolation:isolate;overflow:hidden;background:transparent!important}
    #chatPanel::before{
      content:"";position:absolute;inset:-14px;z-index:-2;
      background-image:var(--fm-chat-theme-image,none);
      background-size:cover;background-position:center;background-repeat:no-repeat;
      filter:blur(7px);transform:scale(1.035);opacity:.92;
    }
    #chatPanel::after{
      content:"";position:absolute;inset:0;z-index:-1;
      background:rgba(255,255,255,.18);pointer-events:none;
    }
    #chatPanel>*{position:relative;z-index:1}
    html.dark #chatPanel::after{background:rgba(7,12,20,.30)}
  `;document.head.appendChild(st);
}
function syncChatRoomTheme(){
  const panel=$('chatPanel');if(!panel)return;
  const sources=[$('homeView'),document.body,document.documentElement].filter(Boolean);
  let bg='none';
  for(const el of sources){
    const value=getComputedStyle(el).backgroundImage;
    if(value&&value!=='none'&&value.includes('url(')){bg=value;break}
  }
  panel.style.setProperty('--fm-chat-theme-image',bg);
}
installChatRoomThemeStyles();
window.addEventListener('load',syncChatRoomTheme,{once:true});

// ===== SPA back navigation =====
// Internal screens use a hash sub-link so browser Back stays inside the app
// instead of leaving the GitHub Pages app.
let routeSyncing=false;
function viewRoute(id){return ({homeView:"home",peopleView:"people",groupsView:"groups",profileView:"profile",settingsView:"settings"})[id]||"home"}
function routeView(route){return ({home:"homeView",people:"peopleView",groups:"groupsView",profile:"profileView",settings:"settingsView"})[route]||"homeView"}
function currentRoute(){
  const h=decodeURIComponent(String(location.hash||"").replace(/^#/,""));
  if(h.startsWith("chat/"))return {type:"chat",id:h.slice(5)};
  return {type:"view",name:h||"home"};
}
function pushAppRoute(route){
  if(routeSyncing)return;
  const target="#"+route;
  if(location.hash===target)return;
  history.pushState({fastMessenger:true,route},"",target);
}
function showView(id,withHistory=true){
  if(!$(id))return;
  if(withHistory)pushAppRoute(viewRoute(id));
  document.querySelectorAll(".view").forEach(x=>x.classList.remove("active"));$(id).classList.add("active");document.querySelectorAll(".nav-item").forEach(x=>x.classList.toggle("active",x.dataset.view===id));
}
function applyAppRoute(){
  routeSyncing=true;
  const r=currentRoute();
  if(r.type==="chat"){
    // Chat is opened by the normal app action; Back simply returns to the
    // previous internal screen without navigating away from the app.
    if(!activeFriend){
      const prior=history.state?.fastMessenger ? history.state.route : "home";
      showView(routeView(prior),false);
    }
  }else{
    if(activeFriend)closeChat();
    showView(routeView(r.name),false);
  }
  routeSyncing=false;
}
function initAppHistory(){
  const r=currentRoute();
  if(!location.hash){
    history.replaceState({fastMessenger:true,route:"home"},"", "#home");
    // Sentinel entry: pressing Back from the app's first screen remains in-app.
    history.pushState({fastMessenger:true,route:"home",sentinel:true},"", "#home");
  }else if(!history.state?.fastMessenger){
    history.replaceState({fastMessenger:true,route:r.name||"home"},"",location.href);
    history.pushState({fastMessenger:true,route:r.name||"home",sentinel:true},"",location.href);
  }
  applyAppRoute();
}
window.addEventListener("popstate",()=>{
  // Keep the app on an internal sub-link when Back reaches its first route.
  if(location.hash){
    if(history.state?.fastMessenger && history.state?.route==="home" && !history.state?.sentinel){
      history.pushState({fastMessenger:true,route:"home",sentinel:true},"", "#home");
    }
    applyAppRoute();
    return;
  }
  // If browser Back reaches the page without an app hash, restore the app route.
  history.pushState({fastMessenger:true,route:"home",sentinel:true},"", "#home");
  applyAppRoute();
});
function syncProfile(){
  if(!profile)return;
  try{saveLocal("profile",profile)}catch(_){}
  const name=profile.displayName||me?.displayName||me?.email?.split("@")[0]||"User";
  $("headerAvatar").src=avatar(profile);
  $("profileAvatar").src=avatar(profile);
  $("profileName").textContent=name;
  $("profileEmail").textContent=profile.email||me?.email||"";
  $("profileBio").textContent=profile.bio||"No bio added.";
  $("editName").value=name;
  $("editPhoto").value=profile.photoURL||"";
  $("editBio").value=profile.bio||"";
}async function ensureUser(){const ref=USERS().doc(me.uid),snap=await ref.get();const base={uid:me.uid,displayName:me.displayName||me.email?.split("@")[0]||"User",email:me.email||"",photoURL:me.photoURL||null,lastSeen:firebase.firestore.FieldValue.serverTimestamp(),online:true};if(!snap.exists)await ref.set(base);else await ref.set({lastSeen:base.lastSeen,online:true},{merge:true});profile={...base,...(snap.exists?snap.data():{})};
  const googleName=me.displayName||profile.displayName||base.displayName;
  const googlePhoto=me.photoURL||profile.photoURL||null;
  const googleEmail=me.email||profile.email||"";
  profile={...profile,displayName:googleName,photoURL:googlePhoto,email:googleEmail};
  await ref.set({displayName:googleName,photoURL:googlePhoto,email:googleEmail},{merge:true});
  syncProfile();syncMenu()
}
function heartbeat(){if(!me)return;const ping=()=>USERS().doc(me.uid).set({online:true,lastSeen:firebase.firestore.FieldValue.serverTimestamp()},{merge:true}).catch(()=>{});ping();clearInterval(heartbeat.t);heartbeat.t=setInterval(ping,30000)}
window.addEventListener("beforeunload",()=>{if(me)USERS().doc(me.uid).set({online:false,lastSeen:firebase.firestore.FieldValue.serverTimestamp()},{merge:true}).catch(()=>{})});
function stopListeners(){
  listUnsubs.forEach(u=>{try{u&&u()}catch(_){}});listUnsubs=[];
  if(notificationUnsub){try{notificationUnsub()}catch(_){} notificationUnsub=null;}
  if(callInviteUnsub){try{callInviteUnsub()}catch(_){} callInviteUnsub=null;}
  if(callUnsub){try{callUnsub()}catch(_){} callUnsub=null;}
  closeChat();
}
function startListeners(){
  stopListeners();
  friendsSyncReady=false;groupsSyncReady=false;messagesSyncReady=false;

  // Always render the Home chat list immediately from the current in-memory/local
  // state. Firebase realtime listeners below will render again when their first
  // snapshots arrive. This prevents the Home UI from depending on the search box.
  hydrateLocalCache();
  renderChats();
  // The RTDB compatibility layer reads collection roots and filters client-side.
  // Keep ONE realtime listener per collection so the home chat list, people list,
  // groups and messages all update immediately without a manual refresh.
  const safeListen=(label,collection,handler)=>{
    try{
      const unsub=collection.onSnapshot(handler,err=>{
        console.warn(`${label} realtime listener`,err);
        // Keep the cached UI alive when a transient RTDB permission/network error occurs.
        hydrateLocalCache();
        // Never require a search-key event to repaint Home after a Firebase error.
        renderChats();
      });
      if(typeof unsub==="function")listUnsubs.push(unsub);
    }catch(err){
      console.warn(`${label} listener setup`,err);
      hydrateLocalCache();
    }
  };

  safeListen("users",USERS(),s=>{
    const liveUsers=s.docs.map(d=>({uid:d.id,...d.data()})).filter(x=>x.uid!==me.uid);
    // Merge realtime users into the warm cache instead of replacing it. This
    // preserves profile fields (especially photoURL) during the first/partial
    // snapshot and prevents chat avatars from disappearing on refresh.
    if(liveUsers.length>0){
      const merged=new Map();
      [...users,...liveUsers].forEach(u=>{
        const uid=String(u?.uid||"");
        if(!uid)return;
        merged.set(uid,{...(merged.get(uid)||{}),...u});
      });
      users=[...merged.values()].filter(x=>String(x.uid)!==String(me.uid));
      saveLocal("users",users);
    }
    renderPeople();renderGroups();renderChats();
  });

  safeListen("friends",FRIENDS(),s=>{
    const liveFriends=s.docs.map(d=>({id:d.id,...d.data()})).filter(x=>String(x.ownerUid)===String(me.uid));
    const cachedRooms=normalizeChatRooms(loadLocal("chatRooms",[]));
    const cachedFriends=cachedRooms.filter(x=>x.kind==="friend").map(x=>x.friend).filter(Boolean);
    // Never let an empty/partial first snapshot erase the last known Home rooms.
    // A later non-empty realtime snapshot replaces the cache with authoritative data.
    if(liveFriends.length>0){
      friends=mergePersistentFriends(liveFriends)||liveFriends;
      friendsSyncReady=true;
      saveLocal("friends",friends);
      const rooms=buildChatRoomCache();
      if(rooms.length)saveLocal("chatRooms",rooms);
    }else{
      const registry=Array.isArray(readJsonKey(persistentFriendsKey(),[]))?readJsonKey(persistentFriendsKey(),[]):[];
      friends=cachedFriends.length?cachedFriends:(registry.length?registry:friends);
      friendsSyncReady=false;
    }
    renderPeople();renderGroups();renderChats();updateStats();
  });

  safeListen("friendRequests",REQUESTS(),s=>{
    const all=s.docs.map(d=>({id:d.id,...d.data()}));
    requests=all.filter(x=>x.receiverUid===me.uid&&x.status==="pending");
    sentRequests=all.filter(x=>x.senderUid===me.uid&&x.status==="pending");
    saveLocal("requests",requests);saveLocal("sentRequests",sentRequests);
    updateRequestBadge();renderPeople();
  });

  safeListen("groups",GROUPS(),s=>{
    const liveGroups=s.docs.map(d=>({id:d.id,...d.data()})).filter(x=>(x.memberUids||[]).some(id=>String(id)===String(me.uid)))
      .sort((a,b)=>(b.createdAt?.toMillis?.()||Number(b.createdAt)||0)-(a.createdAt?.toMillis?.()||Number(a.createdAt)||0));
    // Keep the cached group list visible during a transient empty first snapshot.
    if(liveGroups.length>0 || groups.length===0){
      groups=liveGroups;
      groupsSyncReady=true;
      saveLocal("groups",groups);
    }
    const rooms=buildChatRoomCache();if(rooms.length)saveLocal("chatRooms",rooms);
    renderGroups();renderChats();
  });

  safeListen("messages",MESSAGES(),s=>{
    const all=s.docs.map(d=>({id:d.id,...d.data()}));
    const mine=all.filter(m=>m.senderUid===me.uid||m.receiverUid===me.uid||(Array.isArray(m.groupMemberUids)&&m.groupMemberUids.some(id=>String(id)===String(me.uid))));

    // Keep a warm local message history if the first realtime snapshot is empty.
    // A non-empty snapshot is authoritative and replaces the in-memory state.
    if(mine.length>0 || messageMap.size===0){
      messageMap=new Map(mine.map(m=>[m.id,m]));
      messagesSyncReady=true;
      cacheMessages();
    }

    const rooms=buildChatRoomCache();
    if(rooms.length)saveLocal("chatRooms",rooms);

    // Critical: every snapshot, including the first one, repaints the UI.
    renderChats();
    updateStats();
    if(activeFriend)renderMessages();
  });

  // Final synchronous paint after all listeners have been attached. This also
  // covers the case where Firebase callbacks are delayed by network startup.
  renderChats();
}
function cacheKey(name){return CACHE_PREFIX+name+(me?"_"+me.uid:"")}
function persistentChatRoomsKey(){return me?`${PERSISTENT_ROOMS_PREFIX}${me.uid}`:""}
function persistentFriendsKey(){return me?`${PERSISTENT_FRIENDS_PREFIX}${me.uid}`:""}
function readJsonKey(key,fallback){try{const raw=key&&localStorage.getItem(key);return raw?JSON.parse(raw):fallback}catch(_){return fallback}}
function mergePersistentFriends(list){
  if(!me)return;
  const incoming=Array.isArray(list)?list.filter(Boolean):[];
  const old=Array.isArray(readJsonKey(persistentFriendsKey(),[]))?readJsonKey(persistentFriendsKey(),[]):[];
  const map=new Map();
  [...old,...incoming].forEach(f=>{const uid=String(f?.friendUid||f?.uid||"");if(uid)map.set(uid,{...f,friendUid:f.friendUid||f.uid,ownerUid:f.ownerUid||me.uid})});
  try{localStorage.setItem(persistentFriendsKey(),JSON.stringify([...map.values()]))}catch(_){}
  const rooms=normalizeChatRooms([...normalizeChatRooms(readJsonKey(persistentChatRoomsKey(),[])),...incoming.map(f=>({kind:"friend",friend:{...f,friendUid:f.friendUid||f.uid,ownerUid:f.ownerUid||me.uid}}))]);
  if(rooms.length)try{localStorage.setItem(persistentChatRoomsKey(),JSON.stringify(rooms))}catch(_){}
  return [...map.values()];
}
function readLegacyChatRoomCaches(){
  if(!me)return [];
  const found=[];
  try{
    const suffix=`_chatRooms_${me.uid}`;
    for(let i=0;i<localStorage.length;i++){
      const k=localStorage.key(i);
      if(k&&k.endsWith(suffix)){
        const raw=localStorage.getItem(k);
        const v=raw?JSON.parse(raw):[];
        if(Array.isArray(v))found.push(...v);
      }
    }
  }catch(_){}
  return found;
}
function readLegacyCollectionCaches(name){
  if(!me)return [];
  const found=[];
  try{
    const suffix=`_${name}_${me.uid}`;
    for(let i=0;i<localStorage.length;i++){
      const k=localStorage.key(i);
      if(k&&k.endsWith(suffix)){
        const raw=localStorage.getItem(k);
        const v=raw?JSON.parse(raw):[];
        if(Array.isArray(v))found.push(...v);
      }
    }
  }catch(_){}
  return found;
}
function normalizeChatRooms(list){
  const out=new Map();
  (Array.isArray(list)?list:[]).forEach(x=>{
    if(!x)return;
    if(x.kind==="friend"&&(x.friend?.friendUid||x.friendUid))out.set(`f:${x.friend?.friendUid||x.friendUid}`,{kind:"friend",friend:{...(x.friend||x)}});
    else if(x.kind==="group"&&(x.group?.id||x.group?.uid||x.id))out.set(`g:${x.group?.id||x.group?.uid||x.id}`,{kind:"group",group:{...(x.group||x)}});
  });
  return [...out.values()];
}
function saveLocal(name,value){
  try{localStorage.setItem(cacheKey(name),JSON.stringify(value));
    if(name==="friends"&&me)mergePersistentFriends(value);
    if(name==="chatRooms"&&me){
      const normalized=normalizeChatRooms(value);
      if(normalized.length){
        const existing=normalizeChatRooms(readJsonKey(persistentChatRoomsKey(),[]));
        const merged=normalizeChatRooms([...existing,...normalized]);
        localStorage.setItem(persistentChatRoomsKey(),JSON.stringify(merged));
      }
    }
  }catch(_){}
}
function loadLocal(name,fallback){
  try{
    const raw=localStorage.getItem(cacheKey(name));
    if(raw)return JSON.parse(raw);
    if(name==="friends"&&me){
      const stable=readJsonKey(persistentFriendsKey(),[]);
      if(Array.isArray(stable)&&stable.length)return stable;
      const legacyFriends=readLegacyCollectionCaches("friends");
      if(legacyFriends.length){mergePersistentFriends(legacyFriends);return legacyFriends;}
    }
    if(name==="chatRooms"&&me){
      const stable=readJsonKey(persistentChatRoomsKey(),[]);
      if(Array.isArray(stable)&&stable.length)return stable;
      const legacyStable=readJsonKey(`fm_persistent_chat_rooms_${me.uid}`,[]);
      if(Array.isArray(legacyStable)&&legacyStable.length)return legacyStable;
      const legacy=readLegacyChatRoomCaches();
      if(legacy.length)return normalizeChatRooms(legacy);
    }
    return fallback;
  }catch(_){return fallback}
}
function hydrateLocalCache(){
  if(!me)return;
  const u=loadLocal("users",[]),f=loadLocal("friends",[]),r=loadLocal("requests",[]),sr=loadLocal("sentRequests",[]),g=loadLocal("groups",[]),m=loadLocal("messages",[]);
  if(Array.isArray(u))users=u;
  if(Array.isArray(f)&&f.length)friends=f;
  if(Array.isArray(r))requests=r; if(Array.isArray(sr))sentRequests=sr; if(Array.isArray(g))groups=g;
  messageMap=new Map((Array.isArray(m)?m:[]).map(x=>[x.id,x]));
  const registryFriends=Array.isArray(readJsonKey(persistentFriendsKey(),[]))?readJsonKey(persistentFriendsKey(),[]):[];
  if(!friends.length&&registryFriends.length)friends=registryFriends;
  const cachedRooms=normalizeChatRooms(loadLocal("chatRooms",[]));
  if(cachedRooms.length){
    const cachedFriends=cachedRooms.filter(x=>x.kind==="friend").map(x=>x.friend).filter(Boolean);
    const cachedGroups=cachedRooms.filter(x=>x.kind==="group").map(x=>x.group).filter(Boolean);
    if(!friends.length)friends=cachedFriends;
    if(!groups.length)groups=cachedGroups;
  }
  renderPeople();renderGroups();renderChats();updateStats();
}
function buildChatRoomCache(){
  if(!me)return [];
  const rooms=[];
  const registryFriends=Array.isArray(readJsonKey(persistentFriendsKey(),[]))?readJsonKey(persistentFriendsKey(),[]):[];
  const sourceFriends=friends.length?friends:registryFriends;
  sourceFriends.forEach(f=>{
    const uid=String(f?.friendUid||f?.uid||"");
    const user=uid?(users.find(x=>String(x.uid)===uid)||null):null;
    // Persist the resolved user profile together with the chat room. The RTDB
    // friends record only contains the friendship relation, so without this
    // merge a refresh can temporarily lose the friend's photo until /users
    // finishes loading.
    rooms.push({kind:"friend",friend:{...f,...(user||{}),friendUid:f.friendUid||f.uid,ownerUid:f.ownerUid||me.uid}});
  });
  groups.forEach(g=>rooms.push({kind:"group",group:{...g}}));
  // Also persist people found in message history so a refresh never blanks an existing room.
  const known=new Set(rooms.filter(r=>r.kind==="friend").map(r=>String(r.friend?.friendUid||"")));
  messageMap.forEach(m=>{
    if(m?.groupId)return;
    const uid=String(m?.senderUid===me.uid?m?.receiverUid:m?.senderUid||"");
    if(uid&&!known.has(uid)){
      const u=users.find(x=>String(x.uid)===uid)||friends.find(x=>String(x.friendUid)===uid);
      if(u){rooms.push({kind:"friend",friend:{friendUid:uid,ownerUid:me.uid,...u}});known.add(uid)}
    }
  });
  return normalizeChatRooms(rooms);
}
function cacheMessages(){
  // Keep the latest 1500 messages locally, including text, images, files and call records.
  // Media itself remains at its original URL; the message metadata/URL is available
  // immediately on the next refresh so the UI never has to wait for Firebase to rebuild.
  saveLocal("messages",[...messageMap.values()].slice(-1500));
}
function callDurationPreview(m){
  const video=m.callMode==="video",type=video?"ভিডিও কল":"অডিও কল";
  if(m.callOutcome==="completed")return `${type} · ${callDurationText(m.callDurationMs||0)}`;
  return `মিসড ${type}`;
}
function ensureUnreadChatStyles(){
  if(document.getElementById("fm-unread-chat-styles"))return;
  const style=document.createElement("style");
  style.id="fm-unread-chat-styles";
  style.textContent=`
    .chat-item.unread-chat{
      background:rgba(93, 173, 255, .16) !important;
      background-color:rgba(93, 173, 255, .16) !important;
      border-color:rgba(67, 153, 239, .28) !important;
    }
    .chat-item.unread-chat:hover{
      background:rgba(93, 173, 255, .23) !important;
    }
    .chat-item .unread-badge{
      flex:0 0 auto;
      min-width:23px;
      height:23px;
      padding:0 7px;
      margin-left:8px;
      border-radius:999px;
      display:inline-flex;
      align-items:center;
      justify-content:center;
      background:#2196f3;
      color:#fff;
      font-size:11px;
      font-weight:800;
      line-height:1;
      box-shadow:0 2px 8px rgba(33,150,243,.28);
    }
    .chat-item.unread-chat .item-copy strong{font-weight:800}
    .chat-item .item-meta{margin-left:auto}
    .chat-item .unread-badge + .item-meta{margin-left:4px}
  `;
  document.head.appendChild(style);
}

function isUnreadMessage(m){
  if(!m||m.groupId||!me)return false;
  if(String(m.senderUid)!==String(me.uid)){
    return m.seen!==true && m.read!==true;
  }
  return false;
}

function getUnreadCount(uid){
  if(!uid||!me)return 0;
  let count=0;
  messageMap.forEach(m=>{
    if(String(m.senderUid)===String(uid) && String(m.receiverUid)===String(me.uid) && isUnreadMessage(m))count++;
  });
  return count;
}

async function markConversationRead(uid){
  if(!me||!uid)return;
  const unread=[];
  messageMap.forEach((m,id)=>{
    if(String(m.senderUid)===String(uid) && String(m.receiverUid)===String(me.uid) && isUnreadMessage(m)){
      const next={...m,seen:true,read:true,readAt:Date.now()};
      messageMap.set(id,next);
      unread.push(id);
    }
  });
  if(!unread.length){renderChats();return;}

  cacheMessages();
  try{await idbPutMessages(unread.map(id=>messageMap.get(id)));}catch(e){console.warn("local read state update",e)}
  renderChats();

  await Promise.all(unread.map(async id=>{
    try{
      await MESSAGES().doc(id).update({seen:true,read:true,readAt:firebase.firestore.FieldValue.serverTimestamp()});
    }catch(e){
      console.warn("message read-state sync",id,e);
    }
  }));
}

function renderChats(){
  ensureUnreadChatStyles();
  if(!me)return;

  // Empty/whitespace search means "show all chats". Never let an empty
  // search value filter the list or prevent the initial render.
  const searchValue=$("chatSearch")?.value;
  const q=String(searchValue==null?"":searchValue).trim().toLowerCase();
  const all=[...messageMap.values()].sort((a,b)=>(b.createdAt?.toMillis?.()||Number(b.createdAt)||0)-(a.createdAt?.toMillis?.()||Number(a.createdAt)||0));
  const by=new Map();
  for(const m of all){
    if(m.groupId)continue;
    const uid=m.senderUid===me.uid?m.receiverUid:m.senderUid;
    if(uid&&!by.has(uid))by.set(uid,m);
  }
  // Show every accepted friend in the Home chat list, even before the first message.
  // On a hard refresh, render the last known room list immediately; the realtime
  // listener then replaces it with the current Firebase state.
  const cachedRooms=normalizeChatRooms(loadLocal("chatRooms",[]));
  const cachedFriends=cachedRooms.filter(x=>x.kind==="friend").map(x=>x.friend).filter(Boolean);
  const registryFriends=Array.isArray(readJsonKey(persistentFriendsKey(),[]))?readJsonKey(persistentFriendsKey(),[]):[];
  const roomFriends=friends.length?friends:(cachedFriends.length?cachedFriends:registryFriends);
  roomFriends.forEach(f=>{if(f.friendUid&&!by.has(f.friendUid))by.set(f.friendUid,null)});
  const cachedFriendProfiles=new Map(
    cachedRooms
      .filter(x=>x.kind==="friend")
      .map(x=>[String(x.friend?.friendUid||x.friendUid||""),x.friend])
      .filter(([uid])=>uid)
  );
  let rows=[...by.entries()].map(([uid,m])=>{
    const liveUser=users.find(x=>String(x.uid)===String(uid));
    const friend=friends.find(x=>String(x.friendUid)===String(uid));
    const cachedProfile=cachedFriendProfiles.get(String(uid));
    return {uid,m,u:{...(cachedProfile||{}),...(friend||{}),...(liveUser||{}),uid}};
  });
  if(q.length>0){
    rows=rows.filter(r=>
      String(r.u.displayName||"").toLowerCase().includes(q)||
      String(r.u.email||"").toLowerCase().includes(q)||
      String(r.m?.text||"").toLowerCase().includes(q)
    );
  }
  const box=$("chatList");
  const groupRows=groups.filter(g=>!q||(g.name||"").toLowerCase().includes(q)).map(g=>{
    const m=[...messageMap.values()].filter(x=>x.groupId===g.id).sort((a,b)=>(b.createdAt?.toMillis?.()||0)-(a.createdAt?.toMillis?.()||0))[0];
    const preview=m?.type==="call"?((m.callOutcome==="completed"?"📞 ":"📵 ")+callDurationPreview(m)):m?.text||((m?.imageUrls||[]).length?"📷 ছবি":m?.fileName?"📎 "+m.fileName:"নতুন গ্রুপ");
    return `<button class="chat-item" onclick="openGroupChat('${esc(g.id)}')"><span class="group-chat-icon"><i class="fa-solid fa-user-group"></i></span><span class="item-copy"><strong>${esc(g.name||"Unnamed group")}</strong><small>${esc(preview)}</small></span><time class="item-meta">${m?time(m.createdAt):"Group"}</time></button>`;
  }).join("");
  const personal=rows.map(r=>{
    const preview=r.m?.type==="call"?((r.m.callOutcome==="completed"?"📞 ":"📵 ")+callDurationPreview(r.m)):r.m?.text||((r.m?.imageUrls||[]).length?"📷 Image":r.m?.fileName?"📎 "+r.m.fileName:"Start a conversation");
    const unreadCount=getUnreadCount(r.uid);
    const unreadClass=unreadCount>0?" unread-chat":"";
    const badge=unreadCount>0?`<span class="unread-badge" aria-label="${unreadCount} unread messages">${unreadCount>99?"99+":unreadCount}</span>`:"";
    return `<button class="chat-item${unreadClass}" data-chat-uid="${esc(r.uid)}" onclick="openChat('${esc(r.uid)}')"><img class="avatar" src="${esc(avatar(r.u))}"><span class="item-copy"><strong>${esc(r.u.displayName||r.u.email||"User")}</strong><small>${esc(preview)}</small></span>${badge}<time class="item-meta">${r.m?time(r.m.createdAt):"Friend"}</time></button>`;
  }).join("");
  box.innerHTML=groupRows+personal||`<div class="empty"><i class="fa-regular fa-comments" style="font-size:28px;display:block;margin-bottom:10px"></i>কোনো conversation নেই। People থেকে একজনকে বেছে নিয়ে chat শুরু করুন।</div>`;
  const snapshot=buildChatRoomCache();
  if(snapshot.length)saveLocal("chatRooms",snapshot);
}
function renderGroups(){const box=$("groupList");if(!box)return;const q=($("groupSearch")?.value||"").trim().toLowerCase();const rows=groups.filter(g=>!q||(g.name||"").toLowerCase().includes(q));box.innerHTML=rows.length?rows.map(g=>{const ms=groupMemberUsers(g).slice(0,4);return`<button class="chat-item" onclick="openGroupChat('${esc(g.id)}')"><span class="group-avatar-mini">${ms.map(u=>`<img src="${esc(avatar(u))}" alt="">`).join("")}</span><span class="item-copy"><strong>${esc(g.name||"Unnamed group")}</strong><small>${(g.memberUids||[]).length} জন সদস্য · ${esc((g.memberUids||[]).includes(me.uid)?"আপনি সদস্য":"")}</small></span><span class="item-meta"><i class="fa-solid fa-chevron-right"></i></span></button>`}).join(""):`<div class="empty"><i class="fa-solid fa-user-group" style="font-size:28px;display:block;margin-bottom:10px"></i>এখনও কোনো গ্রুপ নেই।<br>নতুন গ্রুপ তৈরি করে আপনার বন্ধুদের যোগ করুন।</div>`}
function renderGroupPicker(){const box=$("groupFriendPicker"),count=$("groupMemberCount");if(!box)return;const fs=friends.map(f=>users.find(u=>u.uid===f.friendUid)||{uid:f.friendUid,displayName:"User",email:""});if(!fs.length){box.innerHTML='<div class="empty" style="padding:25px 10px;background:transparent;border:0">আগে অন্তত একজন বন্ধুকে Add করুন, তারপর গ্রুপ তৈরি করতে পারবেন।</div>';$("saveGroupBtn").disabled=true;return}box.innerHTML=fs.map(u=>`<label class="group-friend-row"><input type="checkbox" value="${esc(u.uid)}"><img src="${esc(avatar(u))}" alt=""><span class="item-copy"><strong>${esc(u.displayName||"User")}</strong><small>${esc(u.email||"")}</small></span></label>`).join("");const update=()=>{const n=box.querySelectorAll("input:checked").length;count.textContent=`${n} জন নির্বাচিত`;$("saveGroupBtn").disabled=n<1};box.querySelectorAll("input").forEach(x=>x.onchange=update);update()}
function showGroupModal(){if(!me)return;$("groupNameInput").value="";$("groupModal").classList.remove("hidden");renderGroupPicker();setTimeout(()=>$("groupNameInput").focus(),50)}
async function createGroup(){const name=$("groupNameInput").value.trim();const selected=[...document.querySelectorAll("#groupFriendPicker input:checked")].map(x=>x.value);if(!name)return toast("গ্রুপের নাম দিন");if(!selected.length)return toast("অন্তত একজন বন্ধুকে নির্বাচন করুন");if(!selected.every(isFriend))return toast("শুধু আপনার বন্ধুদেরই গ্রুপে যোগ করা যাবে");const btn=$("saveGroupBtn");btn.disabled=true;try{const memberUids=[me.uid,...selected.filter(x=>x!==me.uid)];const ref=GROUPS().doc();await ref.set({name,ownerUid:me.uid,memberUids,createdAt:firebase.firestore.FieldValue.serverTimestamp()});$("groupModal").classList.add("hidden");toast("গ্রুপ তৈরি হয়েছে");showView("groupsView");}catch(e){console.error("createGroup",e);toast(e?.code==="permission-denied"?"গ্রুপ তৈরি করার permission নেই। Firestore rules পরীক্ষা করুন":"গ্রুপ তৈরি করা যায়নি")}finally{btn.disabled=false}}
function renderPeople(){const q=($("peopleSearch")?.value||"").trim().toLowerCase();let rows=peopleTab==="friends"?friends.map(f=>users.find(u=>u.uid===f.friendUid)||{uid:f.friendUid,displayName:"User"}):peopleTab==="requests"?requests.map(r=>users.find(u=>u.uid===r.senderUid)||{uid:r.senderUid,displayName:"User"}):users;if(q)rows=rows.filter(u=>(u.displayName||"").toLowerCase().includes(q)||(u.email||"").toLowerCase().includes(q));const box=$("peopleList");box.innerHTML=rows.length?rows.map(u=>{const pending=requests.find(r=>r.senderUid===u.uid),sent=sentRequests.find(r=>r.receiverUid===u.uid),f=isFriend(u.uid);let actions=f?`<button class="small-btn primary" onclick="openChat('${u.uid}')">Message</button>`:(pending||sent)?`<button class="small-btn" disabled>Pending</button>`:`<button class="small-btn primary" onclick="sendRequest('${u.uid}')">Add friend</button>`;if(peopleTab==="requests"&&pending)actions=`<button class="small-btn primary" onclick="acceptRequest('${pending.id}','${u.uid}')">Accept</button><button class="small-btn danger" onclick="rejectRequest('${pending.id}')">Decline</button>`;return`<div class="person-item"><img class="avatar" src="${esc(avatar(u))}"><div class="item-copy" onclick="openUser('${u.uid}')"><strong>${esc(u.displayName||"User")}</strong><small>${esc(u.email||"")}</small></div><div class="person-actions">${actions}</div></div>`}).join(""):`<div class="empty">কোনো user পাওয়া যায়নি।</div>`}
async function sendRequest(uid){
  if(!me||!uid||uid===me.uid)return;
  if(isFriend(uid))return toast("আপনারা ইতিমধ্যে বন্ধু");
  const ref=REQUESTS().doc(pair(me.uid,uid));
  try{
    const snap=await ref.get();
    if(snap.exists){
      const r=snap.data()||{};
      if(r.status==="pending"){
        if(r.senderUid===me.uid)return toast("Friend request already sent");
        if(r.receiverUid===me.uid)return toast("আপনার কাছে এই বন্ধুত্বের request এসেছে");
      }
      if(r.status==="accepted")return toast("আপনারা ইতিমধ্যে বন্ধু");
      if(r.status!=="rejected")return toast("এই request এখন পরিবর্তন করা যাচ্ছে না");
      if(r.senderUid!==me.uid)return toast("এই request পুনরায় পাঠানোর অনুমতি নেই");
      await ref.update({status:"pending",respondedAt:firebase.firestore.FieldValue.delete(),createdAt:firebase.firestore.FieldValue.serverTimestamp()});
    }else{
      await ref.set({senderUid:me.uid,receiverUid:uid,pairId:pair(me.uid,uid),status:"pending",createdAt:firebase.firestore.FieldValue.serverTimestamp()});
    }
    toast("Friend request sent");
    renderPeople();
  }catch(e){
    console.error("sendRequest",e);
    toast(e?.code==="permission-denied"?"Friend request পাঠানোর permission নেই। Firestore rules পরীক্ষা করুন":"Friend request পাঠানো যায়নি");
  }
}
async function acceptRequest(id,uid){
  if(!me||!id||!uid)return toast("Request পাওয়া যায়নি");
  const reqRef=REQUESTS().doc(id);
  try{
    const reqSnap=await reqRef.get();
    if(!reqSnap.exists)throw new Error("REQUEST_NOT_FOUND");
    const req=reqSnap.data()||{};
    if(req.receiverUid!==me.uid||req.senderUid!==uid)throw new Error("REQUEST_INVALID");
    if(req.status!=="pending")throw new Error("REQUEST_ALREADY_HANDLED");
    const now=firebase.firestore.Timestamp.now();
    const p=pair(me.uid,uid);
    const batch=db.batch();
    batch.set(FRIENDS().doc(p+"__"+me.uid),{pairId:p,ownerUid:me.uid,friendUid:uid,requestId:id,createdAt:now},{merge:true});
    batch.set(FRIENDS().doc(p+"__"+uid),{pairId:p,ownerUid:uid,friendUid:me.uid,requestId:id,createdAt:now},{merge:true});
    batch.set(reqRef,{status:"accepted",respondedAt:now},{merge:true});
    await batch.commit();
    toast("Friend added");
    peopleTab="friends";
    document.querySelectorAll("[data-people-tab]").forEach(x=>x.classList.toggle("active",x.dataset.peopleTab==="friends"));
    renderPeople();
  }catch(e){
    console.error("acceptRequest",e);
    const msg=e?.code==="permission-denied"?"Friend request গ্রহণ করার অনুমতি নেই। আবার চেষ্টা করুন।":e?.message==="REQUEST_NOT_FOUND"?"Request আর পাওয়া যাচ্ছে না":e?.message==="REQUEST_INVALID"?"এই request আপনার জন্য নয়":e?.message==="REQUEST_ALREADY_HANDLED"?"এই request আগে থেকেই সম্পন্ন হয়েছে":"Friend request accept করা যায়নি";
    toast(msg);
  }
}
async function rejectRequest(id){try{const ref=REQUESTS().doc(id),snap=await ref.get();if(!snap.exists||snap.data()?.receiverUid!==me.uid)return toast("Request পাওয়া যায়নি");await ref.set({status:"rejected",respondedAt:firebase.firestore.FieldValue.serverTimestamp()},{merge:true});toast("Request declined")}catch(e){console.error(e);toast("কাজটি করা যায়নি")}}
function updateRequestBadge(){const n=requests.length;["requestBadge","navPeopleBadge"].forEach(id=>{const e=$(id);e.textContent=n;e.classList.toggle("hidden",!n)})}
function updateStats(){if(!me)return;const msgs=[...messageMap.values()];$("statChats").textContent=new Set(msgs.filter(m=>!m.groupId).map(m=>m.senderUid===me.uid?m.receiverUid:m.senderUid)).size;$("statFriends").textContent=friends.length;$("statSent").textContent=msgs.filter(m=>m.senderUid===me.uid).length}
function setChatHeader(u){
  const isGroup=!!u?.isGroup;
  const name=isGroup?(u.name||"Group"):((u.displayName||u.email||"User"));
  const photo=isGroup?"https://placehold.co/120x120/2563eb/ffffff?text=G":avatar(u);
  $("chatAvatar").src=photo;
  $("chatName").textContent=name;
  $("chatStatus").textContent=isGroup?`${(u.memberUids||[]).length} জন সদস্য`:((u.online)?"online":"offline");
  $("chatPresence").classList.toggle("online",!isGroup&&!!u.online);
}

window.openUser=uid=>{const u=users.find(x=>x.uid===uid)||friends.find(x=>x.friendUid===uid);if(!u)return;$("userModalAvatar").src=avatar(u);$("userModalName").textContent=u.displayName||"User";$("userModalEmail").textContent=u.email||"";$("userModalBio").textContent=u.bio||"No bio added.";$("userModalChat").onclick=()=>{closeAllModals();openChat(uid)};$("userModal").classList.remove("hidden")};

function subscribeChat(uid){chatUnsubs.forEach(u=>u&&u());chatUnsubs=[];const ref=MESSAGES();if(activeFriend?.isGroup){chatUnsubs.push(ref.where("groupId","==",uid).onSnapshot(renderMessages));}else{chatUnsubs.push(ref.where("senderUid","==",me.uid).where("receiverUid","==",uid).onSnapshot(renderMessages));chatUnsubs.push(ref.where("senderUid","==",uid).where("receiverUid","==",me.uid).onSnapshot(renderMessages));}}
function messageHTML(m){
  const mine=m.senderUid===me?.uid;
  if(m.type==="call")return `<div class="msg-row ${mine?"mine":"theirs"} call-row" data-message-id="${esc(m.id||"")}"><div class="bubble call-bubble ${m.callOutcome==="missed"||m.callOutcome==="rejected"?"missed":""}"><div class="call-event">${callEventLabel(m)}</div><div class="msg-time">${time(m.createdAt||m.createdAtMs)}</div></div></div>`;
  const imgs=Array.isArray(m.imageUrls)?m.imageUrls:[];
  const legacyFile=m.fileUrl?[{downloadPage:m.fileUrl,id:m.fileId,name:m.fileName,size:m.fileSize,mimetype:m.fileMime}]:[];
  const files=[...(Array.isArray(m.files)?m.files:[]),...legacyFile];
  const uniqueFiles=files.filter((f,i,a)=>f.downloadPage&&a.findIndex(x=>x.downloadPage===f.downloadPage)===i);
  const senderName=users.find(u=>String(u.uid)===String(m.senderUid))?.displayName||"Member";
  const delBtn=`<button class="msg-delete-btn" type="button" title="Delete message" onclick="deleteMessage('${esc(m.id||'')}')"><i class="fa-solid fa-trash-can"></i></button>`;
  return `<div class="msg-row ${mine?"mine":"theirs"}" data-message-id="${esc(m.id||"")}"><div class="bubble">
    ${activeFriend?.isGroup&&!mine?`<div style="font-size:9px;font-weight:800;opacity:.7;margin-bottom:3px">${esc(senderName)}</div>`:""}
    ${m.text?`<div>${esc(m.text).replace(/\n/g,"<br>")}</div>`:""}
    ${imgs.map(u=>`<img class="msg-img" src="${esc(u)}" onclick="showImage('${esc(u)}')">`).join("")}
    ${uniqueFiles.map(f=>`<a class="file-card" href="${esc(f.downloadPage)}" target="_blank" rel="noopener"><span class="file-icon"><i class="fa-solid fa-file-arrow-down"></i></span><span class="file-copy"><b>${esc(f.name||"Shared file")}</b><small>${esc(f.size?bytes(f.size):"File")}</small></span><i class="fa-solid fa-arrow-up-right-from-square file-download"></i></a>`).join("")}
    <div class="msg-footer"><div class="msg-time">${time(m.createdAt||m.createdAtMs)}</div>${delBtn}</div>
  </div></div>`;
}
async function deleteMessage(id){
  if(!me||!id)return;
  const m=activeMessageMap.get(id)||messageMap.get(id);
  if(!m)return toast("Message পাওয়া যায়নি");
  if(m.senderUid!==me.uid&&m.receiverUid!==me.uid)return toast("এই message delete করার অনুমতি নেই");
  if(!confirm("এই message টি delete করবেন?"))return;
  try{
    await MESSAGES().doc(id).delete();
    activeMessageMap.delete(id);messageMap.delete(id);cacheMessages();renderMessages();
    toast("Message deleted");
  }catch(e){
    console.error("deleteMessage",e);
    toast(e?.code==="permission-denied"?"Message delete করার permission নেই। Firestore Rules পরীক্ষা করুন":"Message delete করা যায়নি");
  }
}
function renderMessages(){
  if(!activeFriend)return;
  const arr=[...activeMessageMap.values()].sort((x,y)=>(x.createdAt?.toMillis?.()||Number(x.createdAt)||0)-(y.createdAt?.toMillis?.()||Number(y.createdAt)||0));
  const box=$("messages");
  const escUrl=u=>esc(u||"");
  box.innerHTML=arr.length?arr.map(m=>{
    if(m.type==="call")return `<div class="msg-row ${m.senderUid===me.uid?"mine":"theirs"} call-row" data-message-id="${esc(m.id||"")}"><div class="bubble call-bubble ${m.callOutcome==="missed"||m.callOutcome==="rejected"?"missed":""}"><div class="call-event">${callEventLabel(m)}</div><div class="msg-time">${time(m.createdAt||m.createdAtMs)}</div></div></div>`;
    const mine=m.senderUid===me.uid,imgs=m.imageUrls||[];
    const legacyFile=m.fileUrl?[{downloadPage:m.fileUrl,id:m.fileId,name:m.fileName,size:m.fileSize,mimetype:m.fileMime}]:[];
    const files=[...(Array.isArray(m.files)?m.files:[]),...legacyFile];
    const uniqueFiles=files.filter((f,i,a)=>f.downloadPage&&a.findIndex(x=>x.downloadPage===f.downloadPage)===i);
    return`<div class="msg-row ${mine?"mine":"theirs"}"><div class="bubble">
      ${activeFriend.isGroup&&!mine?`<div style="font-size:9px;font-weight:800;opacity:.7;margin-bottom:3px">${esc(users.find(u=>u.uid===m.senderUid)?.displayName||"Member")}</div>`:""}
      ${m.text?`<div>${esc(m.text).replace(/\n/g,"<br>")}</div>`:""}
      ${imgs.map(u=>`<img class="msg-img" src="${escUrl(u)}" onclick="showImage('${escUrl(u)}')">`).join("")}
      ${uniqueFiles.map(f=>`<a class="file-card" href="${escUrl(f.downloadPage)}" target="_blank" rel="noopener"><span class="file-icon"><i class="fa-solid fa-file-arrow-down"></i></span><span class="file-copy"><b>${esc(f.name||"Shared file")}</b><small>${esc(f.size?bytes(f.size):"File")}</small></span><i class="fa-solid fa-arrow-up-right-from-square file-download"></i></a>`).join("")}
      <div class="msg-time">${time(m.createdAt)}</div>
    </div></div>`;
  }).join(""):"<div class=\"empty\">কোনো message নেই।</div>";
  box.scrollTop=box.scrollHeight;
}
function subscribeChat(uid){
  chatUnsubs.forEach(u=>u&&u());chatUnsubs=[];
  activeMessageMap=new Map([...messageMap.values()].filter(m=>activeFriend?.isGroup?m.groupId===uid:((m.senderUid===me.uid&&m.receiverUid===uid)||(m.senderUid===uid&&m.receiverUid===me.uid))).map(m=>[m.id,m]));
  renderMessages();
  const ref=MESSAGES();
  const mergeSnap=s=>{s.docs.forEach(d=>activeMessageMap.set(d.id,{id:d.id,...d.data()}));renderMessages();};
  if(activeFriend?.isGroup){chatUnsubs.push(ref.where("groupId","==",uid).onSnapshot(mergeSnap));}
  else{
    chatUnsubs.push(ref.where("senderUid","==",me.uid).where("receiverUid","==",uid).onSnapshot(mergeSnap));
    chatUnsubs.push(ref.where("senderUid","==",uid).where("receiverUid","==",me.uid).onSnapshot(mergeSnap));
  }
}
async function openChat(uid){
  if(!me||!uid)return;
  let u=users.find(x=>x.uid===uid)||friends.find(x=>x.friendUid===uid);
  if(!u){
    const cached=[...messageMap.values()].find(m=>m.senderUid===uid||m.receiverUid===uid);
    u={uid,displayName:cached?.senderUid===uid?cached.senderName:cached?.receiverName||"User",email:"",photoURL:null,online:false};
  }
  currentConversationId=pair(me.uid,uid);
  try{await idbOpen()}catch(e){console.warn("local message store unavailable",e)}
  activeFriend=u;
  setChatHeader(u);
  syncChatRoomTheme();
  if(!routeSyncing)pushAppRoute("chat/"+encodeURIComponent(uid));
  $("chatPanel").classList.remove("hidden");
  document.body.style.overflow="hidden";
  subscribeChat(uid);
  watchTyping();
  renderMessages();
  // Opening a personal conversation is the explicit read action.
  if(!activeFriend.isGroup)await markConversationRead(uid);
}
async function openGroupChat(groupId){
  currentConversationId=groupId;
  const g=groups.find(x=>x.id===groupId);if(!g)return toast("গ্রুপ পাওয়া যায়নি");
  if(!(g.memberUids||[]).includes(me.uid))return toast("আপনি এই গ্রুপের সদস্য নন");
  await idbOpen();activeFriend={...g,uid:g.id,isGroup:true};setChatHeader(activeFriend);syncChatRoomTheme();if(!routeSyncing)pushAppRoute("chat/group/"+encodeURIComponent(groupId));$("chatPanel").classList.remove("hidden");document.body.style.overflow="hidden";subscribeChat(groupId);watchTyping();await renderMessages()
}
function closeChat(){currentConversationId=null;chatUnsubs.forEach(u=>u&&u());chatUnsubs=[];if(typingUnsub)typingUnsub();typingUnsub=null;activeFriend=null;$("chatPanel")?.classList.add("hidden");document.body.style.overflow="";attachedImages=[];attachedFiles=[];renderUploadQueue();if(!routeSyncing&&String(location.hash||"").startsWith("#chat/")){history.back()}}
function watchTyping(){if(typingUnsub)typingUnsub();if(!activeFriend||activeFriend.isGroup){$("typing").classList.add("hidden");return}typingUnsub=USERS().doc(activeFriend.uid).onSnapshot(s=>{$("typing").classList.toggle("hidden",(s.data()||{}).typingTo!==me.uid)})}

async function uploadImage(file,onProgress){if(file.size>32*1024*1024)throw new Error("Image 32MB-এর বেশি হতে পারবে না");const fd=new FormData();fd.append("image",file);const r=await fetch(`https://api.imgbb.com/1/upload?key=${IMAGE_UPLOAD_KEY}`,{method:"POST",body:fd});const j=await r.json();if(!j.success)throw new Error("ছবি আপলোড করা যায়নি");if(onProgress)onProgress(100);return j.data.url}

async function uploadFile(file,onProgress){
  const fd=new FormData();fd.append("file",file);
  const r=await fetch(FILE_UPLOAD_ENDPOINT,{method:"POST",body:fd});
  const j=await r.json().catch(()=>null);
  if(!r.ok||!j||j.status!=="ok")throw new Error(j?.status||"ফাইল আপলোড করা যায়নি");
  if(onProgress)onProgress(100);
  return j.data;
}

function renderUploadQueue(){
  const box=$("uploadQueue");
  const items=[
    ...attachedImages.map((f,i)=>({f,type:"image",i})),
    ...attachedFiles.map((f,i)=>({f,type:"file",i}))
  ];
  box.classList.toggle("hidden",!items.length);
  box.innerHTML=items.map(x=>`<div class="queue-chip">
    <span class="queue-thumb">${x.type==="image"
      ?`<img src="${URL.createObjectURL(x.f)}" style="width:34px;height:34px;border-radius:7px;object-fit:cover">`
      :`<i class="fa-solid fa-file-arrow-up"></i>`}</span>
    <span class="queue-copy"><b>${esc(x.f.name)}</b><small>${bytes(x.f.size)}</small><span class="progress"><i></i></span></span>
  </div>`).join("");
}
async function sendMessage(e){
  e.preventDefault();if(!activeFriend||!me)return;
  const input=$("messageInput"),text=input.value.trim();
  if(!text&&!attachedImages.length&&!attachedFiles.length)return;
  if(!activeFriend.isGroup && !isFriend(activeFriend.uid))return toast("আগে Friend Request গ্রহণ হতে হবে, তারপর message পাঠাতে পারবেন");
  if(activeFriend.isGroup && !(activeFriend.memberUids||[]).includes(me.uid))return toast("আপনি এই গ্রুপের সদস্য নন");
  const btn=document.querySelector(".send-btn");btn.disabled=true;
  try{
    const imageUrls=[];
    for(const f of attachedImages){
      toast("ছবি আপলোড হচ্ছে…");
      imageUrls.push(await uploadImage(f));
    }
    const fileDatas=[];
    for(const f of attachedFiles){
      toast("ফাইল আপলোড হচ্ছে…");
      fileDatas.push(await uploadFile(f));
    }
    const firstFile=fileDatas[0]||null;
    const payload={senderUid:me.uid,text,imageUrls,imageUrl:imageUrls[0]||"",files:fileDatas.map(x=>({downloadPage:x.downloadPage||"",id:x.id||"",name:x.name||"",size:x.size||0,mimetype:x.mimetype||""})),fileUrl:firstFile?.downloadPage||"",fileId:firstFile?.id||"",fileName:firstFile?.name||"",fileSize:firstFile?.size||0,fileMime:firstFile?.mimetype||"",fileHost:fileDatas.length?"external":"",createdAt:firebase.firestore.FieldValue.serverTimestamp(),seen:false};
    if(activeFriend.isGroup){payload.groupId=activeFriend.uid;payload.groupMemberUids=activeFriend.memberUids||[];payload.groupMemberMap=Object.fromEntries((activeFriend.memberUids||[]).map(x=>[String(x),true]));}else payload.receiverUid=activeFriend.uid;
    await MESSAGES().add(payload);
    input.value="";input.style.height="auto";
    attachedImages=[];attachedFiles=[];
    renderUploadQueue();toast("Message sent");
  }catch(err){
    console.error(err);
    toast(err.message==="Failed to fetch"?"Upload service blocked or offline":(err.message||"Message পাঠানো যায়নি"));
  }finally{btn.disabled=false;input.focus()}
}
function handleTyping(){if(!activeFriend||activeFriend.isGroup)return;clearTimeout(typingTimer);USERS().doc(me.uid).set({typingTo:activeFriend.uid,typingAt:firebase.firestore.FieldValue.serverTimestamp()},{merge:true}).catch(()=>{});typingTimer=setTimeout(()=>USERS().doc(me.uid).set({typingTo:null},{merge:true}).catch(()=>{}),1200)}
async function saveProfile(){const name=$("editName").value.trim();if(!name)return;try{const photo=$("editPhoto").value.trim()||null,bio=$("editBio").value.trim();await USERS().doc(me.uid).set({displayName:name,photoURL:photo,bio,profileUpdatedAt:firebase.firestore.FieldValue.serverTimestamp()},{merge:true});profile={...profile,displayName:name,photoURL:photo,bio};syncProfile();closeAllModals();toast("Profile updated")}catch(e){toast("Profile save হয়নি")}}



// ===== v4 Offline-first data layer =====
const FM_DB_NAME="fast-messenger-local";
const FM_DB_VERSION=1;
const FM_STORES={messages:"messages",meta:"meta"};
let fmDB=null;
let currentConversationId=null;
let messagePageSize=25;
let oldestLoadedCreatedAt=0;
let syncInProgress=false;

function idbOpen(){
  return new Promise((resolve,reject)=>{
    if(fmDB)return resolve(fmDB);
    const req=indexedDB.open(FM_DB_NAME,FM_DB_VERSION);
    req.onupgradeneeded=e=>{
      const db=e.target.result;
      const ms=db.createObjectStore(FM_STORES.messages,{keyPath:"id"});
      ms.createIndex("conversationCreatedAt",["conversationId","createdAtMs"],{unique:false});
      db.createObjectStore(FM_STORES.meta,{keyPath:"key"});
    };
    req.onsuccess=()=>{fmDB=req.result;resolve(fmDB)};
    req.onerror=()=>reject(req.error);
  });
}
async function idbPut(store,value){
  const db=await idbOpen();
  return new Promise((resolve,reject)=>{
    const tx=db.transaction(store,"readwrite");tx.objectStore(store).put(value);
    tx.oncomplete=resolve;tx.onerror=()=>reject(tx.error);
  });
}
async function idbGet(store,key){
  const db=await idbOpen();
  return new Promise((resolve,reject)=>{
    const tx=db.transaction(store,"readonly"),r=tx.objectStore(store).get(key);
    r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);
  });
}
async function idbMessages(conversationId,limit=25,beforeMs=Infinity){
  const db=await idbOpen();
  return new Promise((resolve,reject)=>{
    const tx=db.transaction(FM_STORES.messages,"readonly");
    const idx=tx.objectStore(FM_STORES.messages).index("conversationCreatedAt");
    const range=IDBKeyRange.bound([conversationId,0],[conversationId,beforeMs]);
    const req=idx.openCursor(range,"prev"),out=[];
    req.onsuccess=e=>{
      const c=e.target.result;
      if(!c||out.length>=limit){resolve(out.reverse());return}
      out.push(c.value);c.continue();
    };
    req.onerror=()=>reject(req.error);
  });
}
async function idbPutMessages(items){
  if(!items?.length)return;
  const db=await idbOpen();
  return new Promise((resolve,reject)=>{
    const tx=db.transaction(FM_STORES.messages,"readwrite"),st=tx.objectStore(FM_STORES.messages);
    items.forEach(m=>st.put(normalizeLocalMessage(m)));
    tx.oncomplete=resolve;tx.onerror=()=>reject(tx.error);
  });
}
async function idbSetMeta(key,value){return idbPut(FM_STORES.meta,{key,value})}
async function idbGetMeta(key){const x=await idbGet(FM_STORES.meta,key);return x?.value}
function normalizeLocalMessage(m){
  return {...m,
    createdAtMs:m.createdAtMs || (m.createdAt?.toMillis?m.createdAt.toMillis():Date.now()),
    conversationId:m.conversationId || pair(m.senderUid,m.receiverUid)
  };
}
async function cacheSnapshotMessages(snapshot){
  const items=snapshot.docs.map(d=>normalizeLocalMessage({id:d.id,...d.data()}));
  await idbPutMessages(items);
  return items;
}
async function renderLocalMessages(conversationId,limit=25,beforeMs=Infinity){
  const local=await idbMessages(conversationId,limit,beforeMs);
  if(conversationId!==currentConversationId)return local;
  oldestLoadedCreatedAt=local.length?local[0].createdAtMs:0;
  renderMessagesFromPlain(local);
  return local;
}
function renderMessagesFromPlain(items){
  const box=$("messages");if(!box)return;
  if(!items.length){
    box.innerHTML='<div class="empty-state"><i class="fa-regular fa-comments"></i><b>No messages yet</b><span>Start the conversation.</span></div>';
    return;
  }
  box.innerHTML=items.map(m=>messageHTML(m)).join("");
  box.scrollTop=box.scrollHeight;
}
async function loadOlderLocalMessages(){
  if(!currentConversationId||!oldestLoadedCreatedAt)return;
  const older=await idbMessages(currentConversationId,messagePageSize,oldestLoadedCreatedAt-1);
  if(!older.length){toast("No more local messages");return}
  const box=$("messages"),oldHeight=box.scrollHeight,oldTop=box.scrollTop;
  box.insertAdjacentHTML("afterbegin",older.map(m=>messageHTML(m)).join(""));
  oldestLoadedCreatedAt=older[0].createdAtMs;
  box.scrollTop=box.scrollHeight-oldHeight+oldTop;
}
async function deltaSync(){
  if(!me||!navigator.onLine||syncInProgress)return;
  syncInProgress=true;setSyncUI(true,"Syncing changes…");
  try{
    const last=Number(await idbGetMeta("lastSync_"+me.uid)||0);
    const cursor=firebase.firestore.Timestamp.fromMillis(last);
    const [s1,s2]=await Promise.all([
      MESSAGES().where("senderUid","==",me.uid).where("createdAt",">",cursor).get(),
      MESSAGES().where("receiverUid","==",me.uid).where("createdAt",">",cursor).get()
    ]);
    const map=new Map();
    s1.docs.forEach(d=>map.set(d.id,{id:d.id,...d.data()}));
    s2.docs.forEach(d=>map.set(d.id,{id:d.id,...d.data()}));
    await idbPutMessages(Array.from(map.values()));
    await idbSetMeta("lastSync_"+me.uid,Date.now());
    if(currentConversationId)await renderLocalMessages(currentConversationId,25);
  }catch(e){console.warn("Delta sync:",e)}
  finally{setSyncUI(false);syncInProgress=false}
}
function setSyncUI(show,text="Syncing…"){
  $("syncBar").classList.toggle("hidden",!show);$("syncText").textContent=text;
}
function updateConnectivity(){
  const off=!navigator.onLine;
  document.body.classList.toggle("offline",off);
  $("offlineBar").classList.toggle("hidden",!off);
  if(!off)deltaSync();
}
const SUPPORT_EMAIL="hkshahadot24@gmail.com";
const DEFAULT_APP_URL=window.location.href.split("#")[0];

function syncMenu(){
  if(!profile)return;
  $("menuAvatar").src=avatar(profile);
  $("menuName").textContent=profile.displayName||me?.displayName||"User";
  $("menuEmail").textContent=profile.email||me?.email||"";
  $("supportEmailLabel").textContent=SUPPORT_EMAIL;
}
function applyTheme(dark,save=true){
  document.documentElement.classList.toggle("dark",dark);
  document.body.classList.toggle("dark",dark);
  $("themeToggle").checked=dark;
  if(save)localStorage.setItem("fm_theme",dark?"dark":"light");
}
function applyNotifications(on,save=true){
  $("notificationToggle").checked=on;
  if(save)localStorage.setItem("fm_notifications",on?"on":"off");
}
function playNotificationSound(){
  if(localStorage.getItem("fm_notifications")==="off")return;
  try{
    const C=window.AudioContext||window.webkitAudioContext;
    if(!C)return;
    const c=new C(),o=c.createOscillator(),g=c.createGain();
    o.frequency.value=880;g.gain.value=.035;o.connect(g);g.connect(c.destination);
    o.start();o.stop(c.currentTime+.08);
  }catch(_){}
}
async function shareApp(){
  const data={title:"Fast Messenger",text:"Fast Messenger-এ যোগ দিন",url:DEFAULT_APP_URL};
  try{
    if(navigator.share)await navigator.share(data);
    else{await navigator.clipboard.writeText(DEFAULT_APP_URL);toast("App link copied")}
  }catch(e){if(e?.name!=="AbortError")toast("Share করা যায়নি")}
}
function supportEmail(){
  if(!SUPPORT_EMAIL||SUPPORT_EMAIL.includes("YOUR_SUPPORT_EMAIL"))return toast("app.js-এ SUPPORT_EMAIL সেট করুন");
  const subject=encodeURIComponent("Fast Messenger Support / Feedback");
  const body=encodeURIComponent(`Hello Support,\n\nআমার সমস্যা / মতামত:\n\n\nUser: ${me?.email||""}`);
  location.href=`mailto:${SUPPORT_EMAIL}?subject=${subject}&body=${body}`;
}
function showSettings(){showView("settingsView");$("moreMenu").classList.add("hidden")}
function showProfile(){showView("profileView");$("moreMenu").classList.add("hidden")}
function confirmDeleteAccount(){
  if(!me)return;
  const ok=confirm("অ্যাকাউন্ট ডিলিট করলে এই অ্যাপের profile, friend list, friend request এবং আপনার messages মুছে যাবে। Google account মুছবে না।\n\nআপনি কি এগোতে চান?");
  if(ok)deleteAccount();
}
async function deleteAccount(){
  const btn=$("deleteAccountBtn");btn.disabled=true;
  try{
    const uid=me.uid;
    const targets=[];
    const snap=await db.ref().once("value");
    const root=snap.val()||{};
    ["users","friends","friendRequests","messages","groups","calls","notifications","typing","presence"].forEach(name=>{
      const node=root[name]||{};
      Object.entries(node).forEach(([id,v])=>{
        const related = name==="users" ? id===uid : name==="friends" ? v?.ownerUid===uid||v?.friendUid===uid : name==="friendRequests" ? v?.senderUid===uid||v?.receiverUid===uid : name==="messages" ? v?.senderUid===uid||v?.receiverUid===uid||v?.groupMemberUids?.includes?.(uid) : name==="groups" ? v?.ownerUid===uid||v?.memberUids?.includes?.(uid) : name==="calls" ? v?.callerUid===uid||v?.receiverUid===uid||v?.recipientUids?.includes?.(uid) : id===uid;
        if(related)targets.push(`${name}/${id}`);
      });
    });
    const updates={};targets.forEach(path=>updates[path]=null);if(Object.keys(updates).length)await db.ref().update(updates);
    await auth.currentUser.delete();
  }catch(e){
    console.error(e);
    if(e?.code==="auth/requires-recent-login")toast("নিরাপত্তার জন্য আবার Google login করে Delete Account চালান");
    else toast("Account delete করা যায়নি");
  }finally{btn.disabled=false}
}

async function clearCache(){
  try{
    localStorage.removeItem("fm_theme");
    localStorage.removeItem("fm_notifications");
    sessionStorage.clear();
    if("caches" in window){const ks=await caches.keys();await Promise.all(ks.map(k=>caches.delete(k)))}
    toast("Temporary cache cleared");
  }catch(e){toast("Cache clear সম্পূর্ণ হয়নি")}
}
function initPreferences(){
  applyTheme(localStorage.getItem("fm_theme")==="dark",false);
  applyNotifications(localStorage.getItem("fm_notifications")!=="off",false);
}

// -----------------------------------------------------------------------------
// Google Sign-in loading states
// -----------------------------------------------------------------------------
// Show an immediate loading state when the user clicks Google sign-in, then
// switch to an authentication state after the Google account has been selected.
// The login screen is finally replaced by the normal app from onAuthStateChanged.
(function initGoogleLoginLoading(){
  const b=$("googleLogin");
  if(!b)return;

  // Inline spinner styles keep this update self-contained in app.js, so no
  // separate CSS file is required.
  if(!document.getElementById("fmGoogleLoginLoaderStyle")){
    const style=document.createElement("style");
    style.id="fmGoogleLoginLoaderStyle";
    style.textContent=`
      .fm-google-loader{
        width:17px;height:17px;border:2px solid currentColor;
        border-right-color:transparent;border-radius:50%;
        display:inline-block;vertical-align:-4px;margin-right:9px;
        animation:fmGoogleSpin .7s linear infinite;
      }
      @keyframes fmGoogleSpin{to{transform:rotate(360deg)}}
      #googleLogin.fm-loading{cursor:wait;opacity:.9}
    `;
    document.head.appendChild(style);
  }

  const originalHtml=b.innerHTML;

  const setLoading=(message)=>{
    b.disabled=true;
    b.classList.add("fm-loading");
    b.setAttribute("aria-busy","true");
    b.innerHTML=`<span class="fm-google-loader" aria-hidden="true"></span><span>${message}</span>`;
  };

  const reset=()=>{
    loginAttempt++;
    clearPopupWatch();
    b.disabled=false;
    b.classList.remove("fm-loading");
    b.removeAttribute("aria-busy");
    b.innerHTML=originalHtml;
  };

  let loginAttempt=0;
  let popupWatchTimer=null;
  let popupWasBlurred=false;

  const clearPopupWatch=()=>{
    if(popupWatchTimer){
      clearTimeout(popupWatchTimer);
      popupWatchTimer=null;
    }
    window.removeEventListener("focus",onWindowFocus,true);
    window.removeEventListener("pageshow",onWindowFocus,true);
  };

  const cancelGoogleLogin=(message="Google account নির্বাচন বাতিল করা হয়েছে। আবার চেষ্টা করুন।")=>{
    clearPopupWatch();
    if(errorBoxRef)errorBoxRef.textContent=message;
    reset();
  };

  let errorBoxRef=null;

  function onWindowFocus(){
    // Returning focus usually means the Google popup was closed. Do not
    // cancel immediately: Firebase may need a short moment to finish the
    // successful OAuth callback. If no auth state arrives, treat it as cancel.
    if(!popupWasBlurred || !b.disabled)return;
    clearTimeout(popupWatchTimer);
    popupWatchTimer=setTimeout(()=>{
      if(b.disabled && !auth.currentUser){
        cancelGoogleLogin();
      }
    },1500);
  }

  b.onclick=async()=>{
    if(b.disabled)return;

    const attempt=++loginAttempt;
    errorBoxRef=$("loginError");
    if(errorBoxRef)errorBoxRef.textContent="";

    popupWasBlurred=false;
    clearPopupWatch();

    // Phase 1: immediately acknowledge the click before opening Google's UI.
    setLoading("Google account খুলছে...");

    // Firebase normally rejects signInWithPopup with auth/popup-closed-by-user.
    // Some browsers/webviews can leave that promise pending after the popup is
    // manually closed, so also watch focus restoration as a reliable fallback.
    const markBlur=()=>{
      if(b.disabled)popupWasBlurred=true;
    };
    window.addEventListener("blur",markBlur,{once:false,capture:true});
    window.addEventListener("focus",onWindowFocus,true);
    window.addEventListener("pageshow",onWindowFocus,true);

    try{
      await auth.setPersistence(firebase.auth.Auth.Persistence.LOCAL);

      if(attempt!==loginAttempt)return;

      const provider=new firebase.auth.GoogleAuthProvider();
      provider.setCustomParameters({prompt:"select_account"});

      // Phase 2 starts as soon as Google returns. Firebase Auth will then
      // trigger the authoritative onAuthStateChanged callback below.
      await auth.signInWithPopup(provider);

      clearPopupWatch();
      setLoading("Google account যাচাই করা হচ্ছে...");
    }catch(e){
      console.error("Google sign-in",e);

      const cancelled=e?.code==="auth/popup-closed-by-user" ||
                      e?.code==="auth/cancelled-popup-request" ||
                      e?.code==="auth/popup-blocked";

      clearPopupWatch();

      if(errorBoxRef){
        errorBoxRef.textContent=cancelled
          ? "Google account নির্বাচন বাতিল করা হয়েছে। আবার চেষ্টা করুন।"
          : "Google account দিয়ে লগইন করা যায়নি। আবার চেষ্টা করুন।";
      }

      reset();
    }finally{
      window.removeEventListener("blur",markBlur,true);
    }
  };

  // Expose a tiny reset hook for the auth callback.
  window.__resetGoogleLoginLoading=reset;
})();
// -----------------------------------------------------------------------------
// Instant offline-first shell
// -----------------------------------------------------------------------------
// The browser can know the last authenticated UID before Firebase Auth finishes
// restoring the session. Use that UID to paint the cached profile/chats immediately.
// The real auth callback below replaces this temporary state with the authoritative
// Firebase user as soon as authentication is restored.
function preloadCachedSession(){
  const cachedUid=localStorage.getItem("fm_session_uid");
  if(!cachedUid)return false;
  try{
    me={uid:cachedUid};
    const rawProfile=localStorage.getItem(`${CACHE_PREFIX}profile_${cachedUid}`);
    profile=rawProfile?JSON.parse(rawProfile):null;
    hydrateLocalCache();
    if(profile)syncProfile();
    syncMenu();
    renderChats();
    return true;
  }catch(e){
    console.warn("cached session preload",e);
    return false;
  }
}

function showCachedShell(){
  const cachedUid=localStorage.getItem("fm_session_uid");
  if(!cachedUid)return false;
  $("loginScreen").classList.add("hidden");
  $("app").classList.remove("hidden");
  document.body.classList.remove("booting");
  return true;
}
const hadCachedSession=showCachedShell();
// Paint the last known session before Firebase Auth finishes restoring the login.
// This removes the blank-profile / blank-chat flash on every hard refresh.
if(hadCachedSession)preloadCachedSession();

auth.onAuthStateChanged(async user=>{
  authResolved=true;
  if(user){
    if(window.__resetGoogleLoginLoading)window.__resetGoogleLoginLoading();
    me=user;localStorage.setItem("fm_session_uid",user.uid);
    $("loginScreen").classList.add("hidden");$("app").classList.remove("hidden");
    document.body.classList.remove("booting");
    profile=loadLocal("profile",{uid:user.uid,displayName:user.displayName||user.email?.split("@")[0]||"User",email:user.email||"",photoURL:user.photoURL||null,bio:"Fast Messenger profile"});
    // Restore every cached collection before any Firebase listener starts.
    syncProfile();syncMenu();hydrateLocalCache();
    renderChats();
    if(typeof requestAnimationFrame==="function")requestAnimationFrame(()=>{hydrateLocalCache();syncProfile();renderChats()});
    heartbeat();startListeners();watchIncomingNotifications();watchCallInvites();
    ensureUser().then(()=>saveLocal("profile",profile)).catch(e=>console.warn("profile sync delayed",e));
  }else{
    if(window.__resetGoogleLoginLoading)window.__resetGoogleLoginLoading();
    localStorage.removeItem("fm_session_uid");
    me=null;profile=null;
    $("app").classList.add("hidden");$("loginScreen").classList.remove("hidden");
    document.body.classList.remove("booting");
  }
});

document.querySelectorAll(".nav-item").forEach(b=>b.onclick=()=>showView(b.dataset.view));
document.querySelectorAll("[data-people-tab]").forEach(b=>b.onclick=()=>{peopleTab=b.dataset.peopleTab;document.querySelectorAll("[data-people-tab]").forEach(x=>x.classList.toggle("active",x===b));renderPeople()});
$("peopleSearch").oninput=renderPeople;$("chatSearch").oninput=renderChats;$("groupSearch").oninput=renderGroups;$("createGroupBtn").onclick=showGroupModal;$("saveGroupBtn").onclick=createGroup;
if($("refreshBtn"))$("refreshBtn").onclick=()=>{renderChats();renderPeople();renderGroups();toast("Refreshed")};
$("backChat").onclick=closeChat;$("composer").onsubmit=sendMessage;
$("messageInput").addEventListener("input",e=>{e.target.style.height="auto";e.target.style.height=Math.min(e.target.scrollHeight,120)+"px";handleTyping()});
$("pickImage").onclick=()=>$("imageInput").click();
$("pickFile").onclick=()=>$("fileInput").click();
$("imageInput").onchange=e=>{
  const incoming=Array.from(e.target.files||[]).filter(f=>f.type.startsWith("image/"));
  attachedImages=[...attachedImages,...incoming].slice(0,10);
  e.target.value="";renderUploadQueue();
};
$("fileInput").onchange=e=>{
  const incoming=Array.from(e.target.files||[]);
  attachedFiles=[...attachedFiles,...incoming].slice(0,10);
  e.target.value="";renderUploadQueue();
};
$("profileBtn").onclick=()=>showProfile();
$("settingsFromProfile").onclick=showSettings;

$("logoutBtn").onclick=async()=>{
  if(!confirm("Log out করবেন?"))return;
  const uid=me?.uid;
  try{
    if(uid){await USERS().doc(uid).set({online:false,lastSeen:firebase.firestore.FieldValue.serverTimestamp()},{merge:true}).catch(e=>console.warn("logout presence update",e));}
  }finally{
    stopListeners();
    try{await auth.signOut();}catch(e){console.error("signOut",e);toast("Logout করা যায়নি");return;}
    localStorage.removeItem("fm_session_uid");
    me=null;profile=null;friends=[];requests=[];sentRequests=[];users=[];groups=[];messageMap.clear();
  }
};
document.querySelectorAll("[data-close]").forEach(b=>b.onclick=()=>$(b.dataset.close).classList.add("hidden"));
$("chatInfo").onclick=()=>{if(!activeFriend)return;if(activeFriend.isGroup){toast(`${activeFriend.name||"Group"} · ${(activeFriend.memberUids||[]).length} জন সদস্য`);return}openUser(activeFriend.uid)};
$("closeLightbox").onclick=()=>{$("lightbox").classList.add("hidden");$("lightboxImg").src=""};


// ===== v3 UI wiring =====
initPreferences();

$("menuBtn").onclick=(e)=>{e.stopPropagation();syncMenu();$("moreMenu").classList.toggle("hidden")};
document.querySelectorAll("[data-menu-action]").forEach(b=>b.onclick=async()=>{
  const a=b.dataset.menuAction;
  if(a==="profile")showProfile();
  if(a==="settings")showSettings();
  if(a==="share"){ $("moreMenu").classList.add("hidden"); await shareApp(); }
  if(a==="support"){ $("moreMenu").classList.add("hidden"); supportEmail(); }
  if(a==="logout"){ $("moreMenu").classList.add("hidden"); $("logoutBtn").click(); }
});
document.addEventListener("click",e=>{
  if(!$("moreMenu").contains(e.target)&&!$("menuBtn").contains(e.target))$("moreMenu").classList.add("hidden");
});
$("settingsBack").onclick=()=>showView("profileView");
$("themeToggle").onchange=e=>applyTheme(e.target.checked);
$("notificationToggle").onchange=e=>applyNotifications(e.target.checked);
$("aboutBtn").onclick=()=>$("aboutModal").classList.remove("hidden");
$("aboutEmailBtn").onclick=supportEmail;
$("privacyBtn").onclick=()=>$("privacyModal").classList.remove("hidden");
$("clearCacheBtn").onclick=clearCache;
$("supportBtn").onclick=supportEmail;
$("accountManagementBtn").onclick=()=>{
  const b=$("accountManagementBtn"),panel=$("accountManagementPanel");
  const open=panel.classList.toggle("hidden")===false;
  b.setAttribute("aria-expanded",open?"true":"false");
};
$("deleteAccountBtn").onclick=confirmDeleteAccount;

// In-app alert/sound for new incoming messages.
let lastKnownIncoming=0;
function watchIncomingNotifications(){
  if(!me)return;
  if(notificationUnsub){try{notificationUnsub()}catch(_){} notificationUnsub=null;}
  notificationUnsub=MESSAGES().where("receiverUid","==",me.uid).onSnapshot(s=>{
    const fresh=s.docChanges().filter(c=>c.type==="added").map(c=>c.doc.data()).filter(m=>Number(m.createdAt||0)>0);
    if(!fresh.length)return;
    const newest=Math.max(...fresh.map(m=>Number(m.createdAt||0)));
    if(lastKnownIncoming && newest>lastKnownIncoming && (!activeFriend || fresh.some(m=>m.senderUid!==activeFriend.uid))){
      playNotificationSound();
      toast("নতুন message এসেছে");
    }
    lastKnownIncoming=Math.max(lastKnownIncoming,newest);
  },e=>console.warn("message notification listener",e));
}

$("audioCallBtn").onclick=()=>startCall("audio");
$("videoCallBtn").onclick=()=>startCall("video");
$("acceptCallBtn").onclick=acceptCall;
$("rejectCallBtn").onclick=rejectIncomingCall;
$("endCallBtn").onclick=()=>endCall(false);
$("muteCallBtn").onclick=toggleMute;
$("cameraCallBtn").onclick=toggleCamera;
$("speakerCallBtn").onclick=openAudioOutputMenu;
$("callParticipantsBtn").onclick=()=>{renderCallParticipants();$("callParticipantsPanel")?.classList.toggle("hidden")};
$("closeCallParticipantsBtn").onclick=closeCallParticipants;
$("callParticipantsList").onclick=e=>{
  const b=e.target.closest("[data-call-person-action]");if(!b)return;
  const uid=b.dataset.uid,action=b.dataset.callPersonAction;
  if(action==="mute")toggleRemoteMute(uid);
  else if(action==="pin")pinCallParticipant(uid);
  else if(action==="remove")removeCallParticipant(uid);
};
document.addEventListener("click",e=>{const menu=$("audioOutputMenu");if(!menu||menu.classList.contains("hidden"))return;if(!menu.contains(e.target)&&!$("speakerCallBtn")?.contains(e.target))menu.classList.add("hidden")});

const originalAuthHandler = auth.currentUser;

// Initialize internal sub-links once the DOM is ready.
if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",initAppHistory,{once:true});else initAppHistory();

// ===== v4 startup hooks =====
window.addEventListener("online",()=>{updateConnectivity();if(me){startListeners();watchIncomingNotifications();watchCallInvites();renderChats();renderPeople();renderGroups();}});
window.addEventListener("offline",updateConnectivity);
document.addEventListener("DOMContentLoaded",async()=>{
  try{await idbOpen()}catch(e){console.warn("IndexedDB unavailable",e)}
  updateConnectivity();
  const box=$("messages");
  if(box)box.addEventListener("scroll",()=>{
    if(box.scrollTop<90 && activeFriend&&!syncInProgress){
      const conversationId=activeFriend.isGroup?activeFriend.uid:pair(me.uid,activeFriend.uid);
      if(conversationId&&oldestLoadedCreatedAt)loadOlderLocalMessages();
    }
  });
});


/* Camera preview controls */
document.addEventListener("click",async e=>{
  const id=e.target.closest("button")?.id;
  if(id==="closeCameraPreviewBtn"){closeCameraPreview();return}
  if(id==="previewMuteBtn"){togglePreviewMute();return}
  if(id==="previewFlipBtn"){await flipPreviewCamera();return}
  if(id==="previewLightBtn"){togglePreviewLight();return}
  if(id==="startVideoCallBtn"){if(pendingVideoCall){const mode=pendingVideoCall.mode;closeCameraPreview();pendingVideoCall=null;await launchCall(mode)}}
});
