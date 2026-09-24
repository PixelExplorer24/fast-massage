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
function messageTimeMs(m){
  return Number(m?.createdAtMs)||rtdbToMillis(m?.createdAt)||Number(m?.timestamp)||0;
}
function isRelevantMessageForMe(m){
  if(!m||!me)return false;
  return String(m.senderUid)===String(me.uid)||String(m.receiverUid)===String(me.uid)||(Array.isArray(m.groupMemberUids)&&m.groupMemberUids.some(id=>String(id)===String(me.uid)));
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
let me=null,profile=null,users=[],friends=[],requests=[],sentRequests=[],groups=[],activeFriend=null,chatUnsubs=[],listUnsubs=[],typingUnsub=null,typingTimer=null,attachedImages=[],attachedFiles=[],messageMap=new Map(),activeMessageMap=new Map(),peopleTab="friends",messageRootUnsub=null;
const CACHE_PREFIX="fm_cache_v13_";
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
  if(messageRootUnsub){try{messageRootUnsub()}catch(_){} messageRootUnsub=null;}
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
    scheduleWarmFriendChatCaches();
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
    scheduleWarmFriendChatCaches();
  });

  // Dedicated RTDB child listeners keep the Home chat list truly realtime.
  // They update only the affected message instead of waiting for a full-root
  // snapshot/query cycle, so a newly arrived message immediately changes its
  // preview, unread count and position on Home.
  try{
    const root=db.ref("messages");
    const upsertMessage=snap=>{
      const raw=snap.val();
      if(!raw||!isRelevantMessageForMe(raw))return;
      const m=normalizeLocalMessage({id:snap.key,...raw});
      messageMap.set(m.id,m);
      cacheMessages();
      renderChats();
      if(activeFriend){
        const belongs=activeFriend.isGroup
          ?String(m.groupId||"")===String(activeFriend.uid||"")
          :((String(m.senderUid)===String(me.uid)&&String(m.receiverUid)===String(activeFriend.uid))||(String(m.senderUid)===String(activeFriend.uid)&&String(m.receiverUid)===String(me.uid)));
        if(belongs){activeMessageMap.set(m.id,m);renderMessages();hydrateRenderedMessageImages([m]).catch(()=>{});}
      }
    };
    const removeMessage=snap=>{
      const id=String(snap.key||"");
      if(!id)return;
      messageMap.delete(id);
      activeMessageMap.delete(id);
      cacheMessages();
      renderChats();
      if(activeFriend)renderMessages();
    };
    const onAdded=s=>upsertMessage(s),onChanged=s=>upsertMessage(s),onRemoved=s=>removeMessage(s);
    root.on("child_added",onAdded,e=>console.warn("messages child_added",e));
    root.on("child_changed",onChanged,e=>console.warn("messages child_changed",e));
    root.on("child_removed",onRemoved,e=>console.warn("messages child_removed",e));
    messageRootUnsub=()=>{root.off("child_added",onAdded);root.off("child_changed",onChanged);root.off("child_removed",onRemoved)};
  }catch(e){
    console.warn("message realtime listener setup",e);
    // Fallback to the existing compatibility listener if direct RTDB listeners fail.
    safeListen("messages",MESSAGES(),s=>{
      const all=s.docs.map(d=>normalizeLocalMessage({id:d.id,...d.data()}));
      const mine=all.filter(isRelevantMessageForMe);
      if(mine.length||messageMap.size===0){messageMap=new Map(mine.map(m=>[m.id,m]));messagesSyncReady=true;cacheMessages()}
      renderChats();updateStats();if(activeFriend)renderMessages();
    });
  }


  // Final synchronous paint after all listeners have been attached. This also
  // covers the case where Firebase callbacks are delayed by network startup.
  renderChats();

  // One-time bootstrap reads complement the realtime listeners. They make the
  // initial Home/People/Groups state deterministic even on a cold load, slow
  // connection, or when the first RTDB value event is delayed.
  Promise.allSettled([
    USERS().get(), FRIENDS().get(), REQUESTS().get(), GROUPS().get(), MESSAGES().get()
  ]).then(([uSnap,fSnap,rSnap,gSnap,mSnap])=>{
    if(!me)return;
    try{
      if(uSnap.status==="fulfilled") users=uSnap.value.docs.map(d=>({uid:d.id,...d.data()})).filter(x=>String(x.uid)!==String(me.uid));
      if(fSnap.status==="fulfilled") {
        const live=fSnap.value.docs.map(d=>({id:d.id,...d.data()})).filter(x=>String(x.ownerUid)===String(me.uid));
        if(live.length) friends=mergePersistentFriends(live)||live;
      }
      if(rSnap.status==="fulfilled") {
        const all=rSnap.value.docs.map(d=>({id:d.id,...d.data()}));
        requests=all.filter(x=>String(x.receiverUid)===String(me.uid)&&x.status==="pending");
        sentRequests=all.filter(x=>String(x.senderUid)===String(me.uid)&&x.status==="pending");
      }
      if(gSnap.status==="fulfilled") {
        const live=gSnap.value.docs.map(d=>({id:d.id,...d.data()})).filter(x=>(x.memberUids||[]).some(id=>String(id)===String(me.uid)));
        if(live.length || groups.length===0) groups=live;
      }
      if(mSnap.status==="fulfilled") {
        const mine=mSnap.value.docs.map(d=>normalizeLocalMessage({id:d.id,...d.data()})).filter(isRelevantMessageForMe);
        if(mine.length || messageMap.size===0) messageMap=new Map(mine.map(m=>[m.id,m]));
      }
      saveLocal("users",users); saveLocal("friends",friends); saveLocal("requests",requests);
      saveLocal("sentRequests",sentRequests); saveLocal("groups",groups); cacheMessages();
      const rooms=buildChatRoomCache(); if(rooms.length)saveLocal("chatRooms",rooms);
      renderPeople(); renderGroups(); renderChats(); updateStats(); updateRequestBadge();
    }catch(e){ console.warn("RTDB bootstrap sync",e); renderChats(); }
  }).catch(e=>console.warn("RTDB bootstrap reads",e));
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
  // Single source of truth for Home chat rendering.
  // This function is intentionally safe to call from:
  // 1) initial page load,
  // 2) Firebase snapshots,
  // 3) search input changes,
  // 4) group creation / refresh.
  const box=$("chatList");
  if(!box)return;
  box.classList.remove("hidden");

  // Auth may not be restored yet. Do not destroy the existing/cached DOM.
  if(!me)return;

  const q=String($("chatSearch")?.value||"").trim().toLowerCase();

  // Warm local state first so a hard refresh never depends on typing in search.
  const cachedRooms=normalizeChatRooms(loadLocal("chatRooms",[]));
  const cachedFriendRooms=cachedRooms.filter(x=>x.kind==="friend").map(x=>x.friend).filter(Boolean);
  const cachedGroupRooms=cachedRooms.filter(x=>x.kind==="group").map(x=>x.group).filter(Boolean);

  // If Firebase has not populated an array yet, recover it from the persistent room cache.
  const roomFriends=friends.length?friends:cachedFriendRooms;
  const roomGroups=groups.length?groups:cachedGroupRooms;

  const all=[...messageMap.values()].sort((a,b)=>messageTimeMs(b)-messageTimeMs(a));

  const by=new Map();
  for(const m of all){
    if(m.groupId)continue;
    const uid=String(m.senderUid)===String(me.uid)?m.receiverUid:m.senderUid;
    if(uid&&!by.has(uid))by.set(String(uid),m);
  }

  // Friends are rooms even when there is no message yet.
  roomFriends.forEach(f=>{
    const uid=String(f?.friendUid||f?.uid||"");
    if(uid&&!by.has(uid))by.set(uid,null);
  });

  const cachedFriendProfiles=new Map(
    cachedFriendRooms
      .map(f=>[String(f?.friendUid||f?.uid||""),f])
      .filter(([uid])=>uid)
  );

  let rows=[...by.entries()].map(([uid,m])=>{
    const liveUser=users.find(x=>String(x.uid)===uid);
    const liveFriend=friends.find(x=>String(x.friendUid)===uid);
    const cachedProfile=cachedFriendProfiles.get(uid);
    return {
      uid,m,
      u:{...(cachedProfile||{}),...(liveFriend||{}),...(liveUser||{}),uid}
    };
  });

  // Empty search MUST mean "show everything".
  if(q){
    rows=rows.filter(r=>
      String(r.u.displayName||"").toLowerCase().includes(q)||
      String(r.u.email||"").toLowerCase().includes(q)||
      String(r.m?.text||"").toLowerCase().includes(q)
    );
  }

  const groupRows=roomGroups
    .filter(g=>!q||String(g?.name||"").toLowerCase().includes(q))
    .map(g=>{
      const m=all
        .filter(x=>String(x.groupId||"")===String(g.id||""))
        .sort((a,b)=>messageTimeMs(b)-messageTimeMs(a))[0];

      const preview=m?.type==="call"
        ?((m.callOutcome==="completed"?"📞 ":"📵 ")+callDurationPreview(m))
        :m?.text||
          ((m?.imageUrls||[]).length?"📷 ছবি":
          m?.fileName?"📎 "+m.fileName:"নতুন গ্রুপ");

      return `<button class="chat-item" data-chat-group="${esc(g.id)}" onclick="openGroupChat('${esc(g.id)}')">
        <span class="group-chat-icon"><i class="fa-solid fa-user-group"></i></span>
        <span class="item-copy"><strong>${esc(g.name||"Unnamed group")}</strong><small>${esc(preview)}</small></span>
        <time class="item-meta">${m?time(m.createdAt):"Group"}</time>
      </button>`;
    }).join("");

  const personal=rows.map(r=>{
    const preview=r.m?.type==="call"
      ?((r.m.callOutcome==="completed"?"📞 ":"📵 ")+callDurationPreview(r.m))
      :r.m?.text||
        ((r.m?.imageUrls||[]).length?"📷 Image":
        r.m?.fileName?"📎 "+r.m.fileName:"Start a conversation");

    const unreadCount=getUnreadCount(r.uid);
    const unreadClass=unreadCount>0?" unread-chat":"";
    const badge=unreadCount>0
      ?`<span class="unread-badge" aria-label="${unreadCount} unread messages">${unreadCount>99?"99+":unreadCount}</span>`
      :"";

    return `<button class="chat-item${unreadClass}" data-chat-uid="${esc(r.uid)}" onclick="openChat('${esc(r.uid)}')">
      <img class="avatar" src="${esc(avatar(r.u))}" alt="">
      <span class="item-copy"><strong>${esc(r.u.displayName||r.u.email||"User")}</strong><small>${esc(preview)}</small></span>
      ${badge}<time class="item-meta">${r.m?time(r.m.createdAt):"Friend"}</time>
    </button>`;
  }).join("");

  // Mix groups and personal chats into one list and sort by the actual latest
  // message transaction. A pinned room (when present in cached room metadata)
  // remains above normal rooms without disturbing realtime ordering.
  const personalRows=rows.map(r=>({kind:"friend",uid:r.uid,m:r.m,u:r.u}));
  const groupData=roomGroups.map(g=>{
    const m=all.find(x=>String(x.groupId||"")===String(g.id||""))||null;
    return {kind:"group",group:g,m};
  }).filter(x=>!q||String(x.group?.name||"").toLowerCase().includes(q));
  const sortRows=(a,b)=>{
    const ap=!!(a.u?.pinned||a.friend?.pinned||a.group?.pinned||a.m?.pinned);
    const bp=!!(b.u?.pinned||b.friend?.pinned||b.group?.pinned||b.m?.pinned);
    if(ap!==bp)return bp-ap;
    return messageTimeMs(b.m)-messageTimeMs(a.m);
  };
  const mixed=[...personalRows,...groupData].sort(sortRows);
  const personalHtmlByUid=new Map(personalRows.map(r=>[String(r.uid),r]));
  const groupHtmlById=new Map(groupRows.match(/data-chat-group="([^"]+)"/g)?.map(x=>[x.match(/"([^"]+)"/)[1],x])||[]);
  const markup=mixed.map(r=>{
    if(r.kind==="friend"){
      const rr=personalHtmlByUid.get(String(r.uid));
      const preview=rr.m?.type==="call"?((rr.m.callOutcome==="completed"?"📞 ":"📵 ")+callDurationPreview(rr.m)):(rr.m?.text||((rr.m?.imageUrls||[]).length?"📷 Image":rr.m?.fileName?"📎 "+rr.m.fileName:"Start a conversation"));
      const unreadCount=getUnreadCount(rr.uid),unreadClass=unreadCount>0?" unread-chat":"",badge=unreadCount>0?`<span class="unread-badge" aria-label="${unreadCount} unread messages">${unreadCount>99?"99+":unreadCount}</span>`:"";
      return `<button class="chat-item${unreadClass}" data-chat-uid="${esc(rr.uid)}" onclick="openChat('${esc(rr.uid)}')"><img class="avatar" src="${esc(avatar(rr.u))}" alt=""><span class="item-copy"><strong>${esc(rr.u.displayName||rr.u.email||"User")}</strong><small>${esc(preview)}</small></span>${badge}<time class="item-meta">${rr.m?time(rr.m.createdAt):"Friend"}</time></button>`;
    }
    const g=r.group,m=r.m,preview=m?.type==="call"?((m.callOutcome==="completed"?"📞 ":"📵 ")+callDurationPreview(m)):m?.text||((m?.imageUrls||[]).length?"📷 ছবি":m?.fileName?"📎 "+m.fileName:"নতুন গ্রুপ");
    return `<button class="chat-item" data-chat-group="${esc(g.id)}" onclick="openGroupChat('${esc(g.id)}')"><span class="group-chat-icon"><i class="fa-solid fa-user-group"></i></span><span class="item-copy"><strong>${esc(g.name||"Unnamed group")}</strong><small>${esc(preview)}</small></span><time class="item-meta">${m?time(m.createdAt):"Group"}</time></button>`;
  }).join("");
  box.innerHTML=markup||`<div class="empty"><i class="fa-regular fa-comments" style="font-size:28px;display:block;margin-bottom:10px"></i>কোনো conversation নেই। People থেকে একজনকে বেছে নিয়ে chat শুরু করুন।</div>`;

  // Persist the same room set used by the renderer, including groups with no messages.
  const snapshot=normalizeChatRooms([
    ...cachedRooms,
    ...buildChatRoomCache()
  ]);
  if(snapshot.length)saveLocal("chatRooms",snapshot);
}

function renderGroups(){const box=$("groupList");if(!box)return;const q=($("groupSearch")?.value||"").trim().toLowerCase();const rows=groups.filter(g=>!q||(g.name||"").toLowerCase().includes(q));box.innerHTML=rows.length?rows.map(g=>{const ms=groupMemberUsers(g).slice(0,4);return`<button class="chat-item" onclick="openGroupChat('${esc(g.id)}')"><span class="group-avatar-mini">${ms.map(u=>`<img src="${esc(avatar(u))}" alt="">`).join("")}</span><span class="item-copy"><strong>${esc(g.name||"Unnamed group")}</strong><small>${(g.memberUids||[]).length} জন সদস্য · ${esc((g.memberUids||[]).includes(me.uid)?"আপনি সদস্য":"")}</small></span><span class="item-meta"><i class="fa-solid fa-chevron-right"></i></span></button>`}).join(""):`<div class="empty"><i class="fa-solid fa-user-group" style="font-size:28px;display:block;margin-bottom:10px"></i>এখনও কোনো গ্রুপ নেই।<br>নতুন গ্রুপ তৈরি করে আপনার বন্ধুদের যোগ করুন।</div>`}
function renderGroupPicker(){const box=$("groupFriendPicker"),count=$("groupMemberCount");if(!box)return;const fs=friends.map(f=>users.find(u=>u.uid===f.friendUid)||{uid:f.friendUid,displayName:"User",email:""});if(!fs.length){box.innerHTML='<div class="empty" style="padding:25px 10px;background:transparent;border:0">আগে অন্তত একজন বন্ধুকে Add করুন, তারপর গ্রুপ তৈরি করতে পারবেন।</div>';$("saveGroupBtn").disabled=true;return}box.innerHTML=fs.map(u=>`<label class="group-friend-row"><input type="checkbox" value="${esc(u.uid)}"><img src="${esc(avatar(u))}" alt=""><span class="item-copy"><strong>${esc(u.displayName||"User")}</strong><small>${esc(u.email||"")}</small></span></label>`).join("");const update=()=>{const n=box.querySelectorAll("input:checked").length;count.textContent=`${n} জন নির্বাচিত`;$("saveGroupBtn").disabled=n<1};box.querySelectorAll("input").forEach(x=>x.onchange=update);update()}
function showGroupModal(){if(!me)return;$("groupNameInput").value="";$("groupModal").classList.remove("hidden");renderGroupPicker();setTimeout(()=>$("groupNameInput").focus(),50)}
async function createGroup(){
  const name=$("groupNameInput").value.trim();
  const selected=[...document.querySelectorAll("#groupFriendPicker input:checked")].map(x=>x.value);

  if(!name)return toast("গ্রুপের নাম দিন");
  if(!selected.length)return toast("অন্তত একজন বন্ধুকে নির্বাচন করুন");
  if(!selected.every(isFriend))return toast("শুধু আপনার বন্ধুদেরই গ্রুপে যোগ করা যাবে");

  const btn=$("saveGroupBtn");
  btn.disabled=true;

  try{
    const memberUids=[me.uid,...selected.filter(x=>x!==me.uid)];
    const ref=GROUPS().doc();
    const groupData={
      name,
      ownerUid:me.uid,
      memberUids,
      createdAt:firebase.firestore.FieldValue.serverTimestamp()
    };

    await ref.set(groupData);

    // Optimistic local update: show the new group immediately instead of
    // waiting for the realtime listener/search input to repaint the Home list.
    const localGroup={...groupData,id:ref.id,createdAt:Date.now()};
    groups=[localGroup,...groups.filter(g=>String(g.id)!==String(ref.id))];

    const rooms=normalizeChatRooms([
      ...normalizeChatRooms(loadLocal("chatRooms",[])),
      {kind:"group",group:localGroup}
    ]);
    saveLocal("groups",groups);
    saveLocal("chatRooms",rooms);

    $("groupModal").classList.add("hidden");
    renderGroups();
    renderChats();
    showView("groupsView");
    toast("গ্রুপ তৈরি হয়েছে");
  }catch(e){
    console.error("createGroup",e);
    toast(e?.code==="permission-denied"?"গ্রুপ তৈরি করার permission নেই। Firestore rules পরীক্ষা করুন":"গ্রুপ তৈরি করা যায়নি");
  }finally{
    btn.disabled=false;
  }
}
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
let replyTarget=null;
function replyPreviewText(m){
  if(m?.text) return String(m.text).replace(/\s+/g," ").trim().slice(0,140);
  if(Array.isArray(m?.imageUrls)&&m.imageUrls.length) return m.imageUrls.length>1?`📷 ${m.imageUrls.length}টি ছবি`:"📷 ছবি";
  if(Array.isArray(m?.files)&&m.files.length) return m.files.length>1?`📎 ${m.files.length}টি ফাইল`:`📎 ${m.files[0]?.name||"ফাইল"}`;
  if(m?.fileName) return `📎 ${m.fileName}`;
  return "Message";
}
function startReply(messageId){
  const m=activeMessageMap.get(messageId)||messageMap.get(messageId);
  if(!m||m.type==="call")return;
  const sender=users.find(u=>String(u.uid)===String(m.senderUid));
  replyTarget={id:messageId,name:String(m.senderUid)===String(me?.uid)?"You":(sender?.displayName||"Member"),text:replyPreviewText(m)};
  renderReplyPreview();
  requestAnimationFrame(()=>{const input=$("messageInput");input?.focus();input?.scrollIntoView?.({block:"nearest"})});
}
function renderReplyPreview(){
  const box=$("replyPreview");if(!box)return;
  if(!replyTarget){box.classList.add("hidden");box.innerHTML="";return;}
  box.innerHTML=`<div class="reply-preview-copy"><span class="reply-preview-label">Replying to</span><b id="replyToName">${esc(replyTarget.name)}</b><span id="replyTextPreview">${esc(replyTarget.text)}</span></div><button type="button" id="cancelReply" class="reply-cancel" aria-label="Cancel reply" title="Cancel reply"><i class="fa-solid fa-xmark"></i></button>`;
  box.classList.remove("hidden");
  box.querySelector("#cancelReply")?.addEventListener("click",cancelReply);
}
function cancelReply(){replyTarget=null;renderReplyPreview();}
function replyQuoteHTML(m){
  if(!m?.replyToId)return "";
  return `<div class="reply-quote" data-reply-target="${esc(m.replyToId)}" onclick="scrollToMessage('${esc(m.replyToId)}')"><b>${esc(m.replyToName||"Member")}</b><span>${esc(m.replyTextPreview||"Message")}</span></div>`;
}
function scrollToMessage(id){
  const row=document.querySelector(`.msg-row[data-message-id="${CSS.escape(String(id))}"]`);
  if(!row)return;
  row.scrollIntoView({behavior:"smooth",block:"center"});
  row.classList.add("reply-highlight");
  setTimeout(()=>row.classList.remove("reply-highlight"),1200);
}
const REACTION_EMOJIS=["❤️","👍","😂","😮","😢","😡"];
let reactionPressTimer=null,reactionPressTarget=null,reactionPressStart=null;
function normalizeReactions(reactions){
  if(!reactions||typeof reactions!=="object")return {};
  return {...reactions};
}
function reactionSummary(reactions){
  const r=normalizeReactions(reactions),out=[];
  Object.entries(r).forEach(([uid,emoji])=>{if(emoji)out.push({uid:String(uid),emoji:String(emoji)})});
  return out;
}
function reactionOverlayHTML(m){
  const list=reactionSummary(m.reactions);
  if(!list.length)return "";
  const counts={};list.forEach(x=>counts[x.emoji]=(counts[x.emoji]||0)+1);
  const parts=Object.entries(counts).map(([emoji,count])=>`<span class="reaction-chip ${emoji==='❤️'?'heart-reaction':''}">${esc(emoji)}${count>1?`<small>${count}</small>`:""}</span>`).join("");
  return `<div class="reaction-overlay" aria-label="Message reactions">${parts}</div>`;
}
function reactionBarHTML(m){
  return `<div class="reaction-bar" data-reaction-bar="${esc(m.id||"")}">${REACTION_EMOJIS.map(e=>`<button type="button" class="reaction-emoji ${e==='❤️'?'heart-reaction':''}" data-reaction="${esc(e)}" data-message-id="${esc(m.id||"")}" aria-label="React ${esc(e)}">${e}</button>`).join("")}</div>`;
}
async function setMessageReaction(messageId,emoji){
  if(!me||!messageId||!emoji)return;
  const m=activeMessageMap.get(messageId)||messageMap.get(messageId); if(!m)return;
  if(m.senderUid!==me.uid && !m.receiverUid && !m.groupMemberMap?.[me.uid] && !activeFriend?.isGroup)return toast("এই message-এ reaction দেওয়ার অনুমতি নেই");
  const reactions=normalizeReactions(m.reactions);
  if(reactions[me.uid]===emoji)delete reactions[me.uid];else reactions[me.uid]=emoji;
  try{
    await MESSAGES().doc(messageId).update({reactions});
    m.reactions=reactions;activeMessageMap.set(messageId,m);messageMap.set(messageId,m);cacheMessages();renderMessages();
  }catch(e){console.error("setMessageReaction",e);toast(e?.code==="permission-denied"?"Reaction দেওয়ার permission নেই":"Reaction দেওয়া যায়নি")}
}
function hideReactionBars(){document.querySelectorAll(".reaction-bar.is-open").forEach(x=>x.classList.remove("is-open"));}
function openReactionBar(row){
  if(!row)return;hideReactionBars();const bar=row.querySelector(".reaction-bar");if(bar){bar.classList.add("is-open");requestAnimationFrame(()=>bar.querySelector(".reaction-emoji")?.focus({preventScroll:true}))}
}
function initMessageReactions(){
  const box=$("messages");if(!box||box.dataset.reactionsBound)return;box.dataset.reactionsBound="1";
  box.addEventListener("pointerdown",e=>{
    const row=e.target.closest(".msg-row[data-message-id]");if(!row||e.target.closest(".reaction-bar")||e.target.closest("a,button"))return;
    reactionPressTarget=row;reactionPressStart={x:e.clientX,y:e.clientY};clearTimeout(reactionPressTimer);
    reactionPressTimer=setTimeout(()=>{if(reactionPressTarget===row)openReactionBar(row)},520);
  },{passive:true});
  ["pointerup","pointercancel","pointerleave"].forEach(ev=>box.addEventListener(ev,e=>{clearTimeout(reactionPressTimer);if(ev!=="pointerleave")reactionPressTarget=null},{passive:true}));
  box.addEventListener("contextmenu",e=>{const row=e.target.closest(".msg-row[data-message-id]");if(!row)return;e.preventDefault();openReactionBar(row)});
  box.addEventListener("click",e=>{
    const b=e.target.closest(".reaction-emoji");
    if(b){e.preventDefault();e.stopPropagation();setMessageReaction(b.dataset.messageId,b.dataset.reaction);return;}
    if(!e.target.closest(".reaction-bar"))hideReactionBars();
  });
}

function messageHTML(m){
  const mine=m.senderUid===me?.uid;
  if(m.type==="call")return `<div class="msg-row ${mine?"mine":"theirs"} call-row" data-message-id="${esc(m.id||"")}"><div class="bubble call-bubble ${m.callOutcome==="missed"||m.callOutcome==="rejected"?"missed":""}"><div class="call-event">${callEventLabel(m)}</div><div class="msg-time">${time(m.createdAt||m.createdAtMs)}</div></div></div>`;
  const imgs=Array.isArray(m.imageUrls)?m.imageUrls:[];
  const legacyFile=m.fileUrl?[{downloadPage:m.fileUrl,id:m.fileId,name:m.fileName,size:m.fileSize,mimetype:m.fileMime}]:[];
  const files=[...(Array.isArray(m.files)?m.files:[]),...legacyFile];
  const uniqueFiles=files.filter((f,i,a)=>f.downloadPage&&a.findIndex(x=>x.downloadPage===f.downloadPage)===i);
  const senderName=users.find(u=>String(u.uid)===String(m.senderUid))?.displayName||"Member";
  const delBtn=`<button class="msg-delete-btn" type="button" title="Delete message" onclick="deleteMessage('${esc(m.id||'')}')"><i class="fa-solid fa-trash-can"></i></button>`;
  return `<div class="msg-row ${mine?"mine":"theirs"}" data-message-id="${esc(m.id||"")}"><div class="bubble">${reactionBarHTML(m)}
    ${activeFriend?.isGroup&&!mine?`<div style="font-size:9px;font-weight:800;opacity:.7;margin-bottom:3px">${esc(senderName)}</div>`:""}
    ${replyQuoteHTML(m)}
    ${m.text?`<div>${esc(m.text).replace(/\n/g,"<br>")}</div>`:""}
    ${imgs.map(u=>{
      const prefetched=!!m.fmPrefetched;
      return prefetched
        ? `<img class="msg-img fm-prefetched-image" data-image-url="${esc(u)}" src="${esc(fmImageObjectUrls.get(u)||u)}" loading="eager" decoding="async" title="Original image download করতে ক্লিক করুন" onclick="downloadOriginalImage(this.dataset.imageUrl)">`
        : `<img class="msg-img" data-image-url="${esc(u)}" src="${esc(fmImageObjectUrls.get(u)||u)}" loading="eager" decoding="async" onclick="event.stopPropagation();showImage('${esc(u)}')">`;
    }).join("")}
    ${uniqueFiles.map(f=>`<a class="file-card" href="${esc(f.downloadPage)}" target="_blank" rel="noopener"><span class="file-icon"><i class="fa-solid fa-file-arrow-down"></i></span><span class="file-copy"><b>${esc(f.name||"Shared file")}</b><small>${esc(f.size?bytes(f.size):"File")}</small></span><i class="fa-solid fa-arrow-up-right-from-square file-download"></i></a>`).join("")}
    ${reactionOverlayHTML(m)}<div class="msg-footer"><div class="msg-time">${time(m.createdAt||m.createdAtMs)}</div><div class="msg-actions"><button class="msg-reply-btn" type="button" title="Reply" onclick="event.stopPropagation();startReply('${esc(m.id||"")}')"><i class="fa-solid fa-reply"></i></button>${delBtn}</div></div>
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
  const arr=[...activeMessageMap.values()].sort((x,y)=>messageTimeMs(x)-messageTimeMs(y));
  const box=$("messages");
  const escUrl=u=>esc(u||"");
  const oldHeight=box.scrollHeight,oldTop=box.scrollTop;
  const wasAtBottom=(oldHeight-box.clientHeight-oldTop)<72 || oldHeight===0;
  box.innerHTML=arr.length?arr.map(m=>{
    if(m.type==="call")return `<div class="msg-row ${m.senderUid===me.uid?"mine":"theirs"} call-row" data-message-id="${esc(m.id||"")}"><div class="bubble call-bubble ${m.callOutcome==="missed"||m.callOutcome==="rejected"?"missed":""}"><div class="call-event">${callEventLabel(m)}</div><div class="msg-time">${time(m.createdAt||m.createdAtMs)}</div></div></div>`;
    const mine=m.senderUid===me.uid,imgs=m.imageUrls||[];
    const legacyFile=m.fileUrl?[{downloadPage:m.fileUrl,id:m.fileId,name:m.fileName,size:m.fileSize,mimetype:m.fileMime}]:[];
    const files=[...(Array.isArray(m.files)?m.files:[]),...legacyFile];
    const uniqueFiles=files.filter((f,i,a)=>f.downloadPage&&a.findIndex(x=>x.downloadPage===f.downloadPage)===i);
    return`<div class="msg-row ${mine?"mine":"theirs"}" data-message-id="${esc(m.id||"")}"><div class="bubble">${reactionBarHTML(m)}
      ${activeFriend.isGroup&&!mine?`<div style="font-size:9px;font-weight:800;opacity:.7;margin-bottom:3px">${esc(users.find(u=>u.uid===m.senderUid)?.displayName||"Member")}</div>`:""}
      ${replyQuoteHTML(m)}
      ${m.text?`<div>${esc(m.text).replace(/\n/g,"<br>")}</div>`:""}
      ${imgs.map(u=>`<div class="media-bubble ${m.localPending?"media-pending":""}"><img class="msg-img" data-image-url="${escUrl(u)}" src="${escUrl(fmImageObjectUrls.get(u)||u)}" loading="eager" decoding="async" onclick="event.stopPropagation();showImage('${escUrl(u)}')"><div class="media-overlay-actions"><button type="button" title="Zoom" onclick="event.stopPropagation();showImage('${escUrl(u)}')"><i class="fa-solid fa-magnifying-glass-plus"></i></button><button type="button" title="Download" onclick="event.stopPropagation();downloadOriginalImage('${escUrl(u)}')"><i class="fa-solid fa-download"></i></button></div>${m.localPending?`<span class="media-uploading"><i class="fa-solid fa-spinner fa-spin"></i> Uploading…</span>`:""}</div>`).join("")}
      ${uniqueFiles.map(f=>f.downloadPage?`<a class="file-card" href="${escUrl(f.downloadPage)}" target="_blank" rel="noopener"><span class="file-icon"><i class="fa-solid fa-file-arrow-down"></i></span><span class="file-copy"><b>${esc(f.name||"Shared file")}</b><small>${esc(f.size?bytes(f.size):"File")}</small></span><i class="fa-solid fa-download file-download"></i></a>`:`<div class="file-card pending-file"><span class="file-icon"><i class="fa-solid fa-file-arrow-up"></i></span><span class="file-copy"><b>${esc(f.name||"File")}</b><small>${esc(f.size?bytes(f.size):"File")} • Uploading…</small></span><i class="fa-solid fa-spinner fa-spin file-download"></i></div>`).join("")}
      ${reactionOverlayHTML(m)}<div class="msg-footer"><div class="msg-time">${time(m.createdAt)}</div><div class="msg-actions"><button class="msg-reply-btn" type="button" title="Reply" onclick="event.stopPropagation();startReply('${esc(m.id||"")}')"><i class="fa-solid fa-reply"></i></button><button class="msg-delete-btn" type="button" title="Delete message" onclick="event.stopPropagation();deleteMessage('${esc(m.id||"")}')"><i class="fa-solid fa-trash-can"></i></button></div></div>
    </div></div>`;
  }).join(""):"<div class=\"empty\">কোনো message নেই।</div>";
  if(wasAtBottom)box.scrollTop=box.scrollHeight;
  else box.scrollTop=Math.max(0,oldTop+(box.scrollHeight-oldHeight));
}
function subscribeChat(uid){
  chatUnsubs.forEach(u=>u&&u());chatUnsubs=[];
  activeMessageMap=new Map([...messageMap.values()].filter(m=>activeFriend?.isGroup?m.groupId===uid:((m.senderUid===me.uid&&m.receiverUid===uid)||(m.senderUid===uid&&m.receiverUid===me.uid))).map(m=>[m.id,m]));
  renderMessages();
  const ref=MESSAGES();
  const mergeSnap=s=>{
    const items=s.docs.map(d=>normalizeLocalMessage({id:d.id,...d.data()}));
    items.forEach(m=>{
      // Reconcile the optimistic local message with the authoritative Firestore message.
      // The clientMessageId is generated before upload, so the sender can replace the
      // "Uploading…" bubble immediately when the write becomes visible in onSnapshot.
      if(m.clientMessageId){
        const pendingId=String(m.clientMessageId);
        const pending=activeMessageMap.get(pendingId)||messageMap.get(pendingId);
        if(pending?.localPending){
          activeMessageMap.delete(pendingId);
          messageMap.delete(pendingId);
          (pending.imageUrls||[]).forEach(u=>{try{if(String(u).startsWith('blob:'))URL.revokeObjectURL(u)}catch(_){}});
        }
      }
      activeMessageMap.set(m.id,m);
      messageMap.set(m.id,m);
    });
    idbPutMessages(items).catch(()=>{});
    preloadMessageImages(items).catch(()=>{});
    renderMessages();
    hydrateRenderedMessageImages(items).catch(()=>{});
  };
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
  historyPage=1;historyNoMore=false;historyPrefetchBusy=false;oldestLoadedCreatedAt=0;
  // Offline-first: paint the most recent locally cached messages first.
  // Awaiting this prevents the async local render from racing with the realtime
  // subscription and jumping the viewport from the top toward the bottom.
  const localConversationId=pair(me.uid,uid);
  try{await renderLocalMessages(localConversationId,FM_WARM_MESSAGE_LIMIT)}catch(_){renderMessages()}
  setChatHeader(u);
  syncChatRoomTheme();
  if(!routeSyncing)pushAppRoute("chat/"+encodeURIComponent(uid));
  $("chatPanel").classList.remove("hidden");
  document.body.style.overflow="hidden";
  subscribeChat(uid);
  watchTyping();
  renderMessages();
  // Entering a chat always starts at the newest message immediately — no smooth
  // top-to-bottom animation and no delayed jump after the realtime snapshot.
  const jumpToLatest=()=>{const box=$("messages");if(box){box.style.scrollBehavior="auto";box.scrollTop=box.scrollHeight;requestAnimationFrame(()=>{box.scrollTop=box.scrollHeight})}};
  jumpToLatest();
  // Opening a personal conversation is the explicit read action.
  if(!activeFriend.isGroup)await markConversationRead(uid);
}
async function openGroupChat(groupId){
  currentConversationId=groupId;
  const g=groups.find(x=>x.id===groupId);if(!g)return toast("গ্রুপ পাওয়া যায়নি");
  if(!(g.memberUids||[]).includes(me.uid))return toast("আপনি এই গ্রুপের সদস্য নন");
  await idbOpen();activeFriend={...g,uid:g.id,isGroup:true};
  historyPage=1;historyNoMore=false;historyPrefetchBusy=false;oldestLoadedCreatedAt=0;
  // Group cache uses a dedicated conversation key so it survives refresh.
  try{await renderLocalMessages(`group:${groupId}`,FM_WARM_MESSAGE_LIMIT)}catch(_){renderMessages()}
  setChatHeader(activeFriend);syncChatRoomTheme();if(!routeSyncing)pushAppRoute("chat/group/"+encodeURIComponent(groupId));$("chatPanel").classList.remove("hidden");document.body.style.overflow="hidden";subscribeChat(groupId);watchTyping();renderMessages();
  const jumpToLatest=()=>{const box=$("messages");if(box){box.style.scrollBehavior="auto";box.scrollTop=box.scrollHeight;requestAnimationFrame(()=>{box.scrollTop=box.scrollHeight})}};jumpToLatest()
}
function closeChat(){currentConversationId=null;chatUnsubs.forEach(u=>u&&u());chatUnsubs=[];if(typingUnsub)typingUnsub();typingUnsub=null;activeFriend=null;$("chatPanel")?.classList.add("hidden");document.body.style.overflow="";attachedImages=[];attachedFiles=[];replyTarget=null;renderUploadQueue();renderReplyPreview();if(!routeSyncing&&String(location.hash||"").startsWith("#chat/")){history.back()}}
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
  if(e?.preventDefault)e.preventDefault();
  if(!activeFriend||!me)return;
  const input=$("messageInput");
  const keepComposerFocus=document.activeElement===input || !!input?.matches?.(":focus");
  const selectionStart=input?.selectionStart ?? null;
  const selectionEnd=input?.selectionEnd ?? null;
  const text=input.value.trim();
  const currentReply=replyTarget?{...replyTarget}:null;
  const imageFiles=[...attachedImages];
  const fileFiles=[...attachedFiles];
  if(!text&&!imageFiles.length&&!fileFiles.length)return;
  if(!activeFriend.isGroup && !isFriend(activeFriend.uid))return toast("আগে Friend Request গ্রহণ হতে হবে, তারপর message পাঠাতে পারবেন");
  if(activeFriend.isGroup && !(activeFriend.memberUids||[]).includes(me.uid))return toast("আপনি এই গ্রুপের সদস্য নন");

  const btn=document.querySelector(".send-btn");
  // Do not disable the send button while media uploads in the background.
  // Each send owns its own optimistic message and upload promise, so users can
  // immediately send another message/media item without waiting.
  const pendingId=`pending_${me.uid}_${Date.now()}_${Math.random().toString(36).slice(2,8)}`;
  const localImageUrls=imageFiles.map(f=>URL.createObjectURL(f));
  const localFiles=fileFiles.map(f=>({name:f.name,size:f.size,mimetype:f.type,downloadPage:"",localOnly:true}));
  const conversationId=activeFriend.isGroup?`group:${activeFriend.uid}`:pair(me.uid,activeFriend.uid);
  const pending={
    id:pendingId,senderUid:me.uid,text,imageUrls:localImageUrls,imageUrl:localImageUrls[0]||"",files:localFiles,
    createdAtMs:Date.now(),createdAt:new Date(),localPending:true,pendingStatus:"uploading",
    conversationId,groupId:activeFriend.isGroup?activeFriend.uid:undefined,
    groupMemberUids:activeFriend.isGroup?(activeFriend.memberUids||[]):undefined,
    receiverUid:activeFriend.isGroup?undefined:activeFriend.uid,
    replyToId:currentReply?.id||"",replyToName:currentReply?.name||"",replyTextPreview:currentReply?.text||""
  };

  // Optimistic UI: detach the selected files immediately so the composer never waits for upload.
  attachedImages=[];attachedFiles=[];input.value="";input.style.height="auto";
  replyTarget=null;renderReplyPreview();
  activeMessageMap.set(pendingId,pending);messageMap.set(pendingId,pending);
  renderUploadQueue();renderMessages();
  requestAnimationFrame(()=>{const box=$("messages");if(box)box.scrollTop=box.scrollHeight;});

  try{
    const imageUrls=[];
    for(const f of imageFiles)imageUrls.push(await uploadImage(f));
    const fileDatas=[];
    for(const f of fileFiles)fileDatas.push(await uploadFile(f));
    const firstFile=fileDatas[0]||null;
    const payload={senderUid:me.uid,clientMessageId:pendingId,text,replyToId:currentReply?.id||"",replyToName:currentReply?.name||"",replyTextPreview:currentReply?.text||"",imageUrls,imageUrl:imageUrls[0]||"",files:fileDatas.map(x=>({downloadPage:x.downloadPage||"",id:x.id||"",name:x.name||"",size:x.size||0,mimetype:x.mimetype||""})),fileUrl:firstFile?.downloadPage||"",fileId:firstFile?.id||"",fileName:firstFile?.name||"",fileSize:firstFile?.size||0,fileMime:firstFile?.mimetype||"",fileHost:fileDatas.length?"external":"",createdAt:firebase.firestore.FieldValue.serverTimestamp(),seen:false};
    if(activeFriend.isGroup){payload.groupId=activeFriend.uid;payload.groupMemberUids=activeFriend.memberUids||[];payload.groupMemberMap=Object.fromEntries((activeFriend.memberUids||[]).map(x=>[String(x),true]));}else payload.receiverUid=activeFriend.uid;
    const docRef=await MESSAGES().add(payload);

    // Replace the local preview with the real Firebase message. The visual message stays in place.
    activeMessageMap.delete(pendingId);messageMap.delete(pendingId);
    localImageUrls.forEach(u=>URL.revokeObjectURL(u));
    toast("Message sent");
    try{
      const real=normalizeLocalMessage({id:docRef.id,...payload,createdAtMs:Date.now()});
      // The local optimistic bubble has already been removed above. Keep the real
      // message in memory until the listener confirms it, avoiding any visual gap.
      activeMessageMap.set(real.id,real);messageMap.set(real.id,real);
      await idbPutMessages([real]);
    }catch(_){ }
    renderMessages();
  }catch(err){
    console.error(err);
    pending.pendingStatus="failed";pending.localPending=true;
    activeMessageMap.set(pendingId,pending);messageMap.set(pendingId,pending);
    renderMessages();
    toast(err.message==="Failed to fetch"?"Upload service blocked or offline":(err.message||"File/message পাঠানো যায়নি"));
  }finally{
    if(keepComposerFocus && input && !input.disabled){
      requestAnimationFrame(()=>{try{input.focus({preventScroll:true});if(selectionStart!==null&&document.activeElement===input){const pos=Math.min(input.value.length,selectionStart);input.setSelectionRange(pos,pos)}}catch(_){try{input.focus()}catch(__){}}});
    }
  }
}
function handleTyping(){if(!activeFriend||activeFriend.isGroup)return;clearTimeout(typingTimer);USERS().doc(me.uid).set({typingTo:activeFriend.uid,typingAt:firebase.firestore.FieldValue.serverTimestamp()},{merge:true}).catch(()=>{});typingTimer=setTimeout(()=>USERS().doc(me.uid).set({typingTo:null},{merge:true}).catch(()=>{}),1200)}
async function saveProfile(){const name=$("editName").value.trim();if(!name)return;try{const photo=$("editPhoto").value.trim()||null,bio=$("editBio").value.trim();await USERS().doc(me.uid).set({displayName:name,photoURL:photo,bio,profileUpdatedAt:firebase.firestore.FieldValue.serverTimestamp()},{merge:true});profile={...profile,displayName:name,photoURL:photo,bio};syncProfile();closeAllModals();toast("Profile updated")}catch(e){toast("Profile save হয়নি")}}



// ===== v4 Offline-first data layer =====
const FM_DB_NAME="fast-messenger-local";
const FM_DB_VERSION=2;
const FM_STORES={messages:"messages",meta:"meta",images:"images"};
const FM_PAGE_SIZE=25;
const FM_WARM_PAGES=2;
const FM_WARM_MESSAGE_LIMIT=FM_PAGE_SIZE*FM_WARM_PAGES;
let fmWarmupPromise=null;
const fmImageObjectUrls=new Map();
let fmDB=null;
let currentConversationId=null;
let messagePageSize=25;
let oldestLoadedCreatedAt=0;
let historyPage=1;
let historyPrefetchBusy=false;
let historyNoMore=false;
let syncInProgress=false;

function idbOpen(){
  return new Promise((resolve,reject)=>{
    if(fmDB)return resolve(fmDB);
    const req=indexedDB.open(FM_DB_NAME,FM_DB_VERSION);
    req.onupgradeneeded=e=>{
      const db=e.target.result;
      let ms=db.objectStoreNames.contains(FM_STORES.messages)?e.target.transaction.objectStore(FM_STORES.messages):db.createObjectStore(FM_STORES.messages,{keyPath:"id"});
      if(!ms.indexNames.contains("conversationCreatedAt"))ms.createIndex("conversationCreatedAt",["conversationId","createdAtMs"],{unique:false});
      if(!db.objectStoreNames.contains(FM_STORES.meta))db.createObjectStore(FM_STORES.meta,{keyPath:"key"});
      if(!db.objectStoreNames.contains(FM_STORES.images)){
        db.createObjectStore(FM_STORES.images,{keyPath:"url"});
      }
    };
    req.onsuccess=()=>{
      fmDB=req.result;
      migrateLocalMessagesV2(fmDB).then(()=>resolve(fmDB)).catch(()=>resolve(fmDB));
    };
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
  const conversationId=m.conversationId || (m.groupId?`group:${m.groupId}`:pair(m.senderUid,m.receiverUid));
  return {...m,
    createdAtMs:m.createdAtMs || (m.createdAt?.toMillis?m.createdAt.toMillis():Date.now()),
    conversationId
  };
}
async function migrateLocalMessagesV2(db){
  const done=await new Promise(resolve=>{
    const tx=db.transaction(FM_STORES.meta,"readonly"),r=tx.objectStore(FM_STORES.meta).get("messages_v2_migrated");
    r.onsuccess=()=>resolve(!!r.result?.value);r.onerror=()=>resolve(false);
  });
  if(done)return;
  const rows=await new Promise((resolve,reject)=>{
    const tx=db.transaction(FM_STORES.messages,"readonly"),r=tx.objectStore(FM_STORES.messages).getAll();
    r.onsuccess=()=>resolve(r.result||[]);r.onerror=()=>reject(r.error);
  });
  if(rows.length){
    await new Promise((resolve,reject)=>{
      const tx=db.transaction(FM_STORES.messages,"readwrite"),st=tx.objectStore(FM_STORES.messages);
      rows.forEach(m=>st.put(normalizeLocalMessage(m)));
      tx.oncomplete=resolve;tx.onerror=()=>reject(tx.error);
    });
  }
  await new Promise((resolve,reject)=>{
    const tx=db.transaction(FM_STORES.meta,"readwrite");tx.objectStore(FM_STORES.meta).put({key:"messages_v2_migrated",value:true});
    tx.oncomplete=resolve;tx.onerror=()=>reject(tx.error);
  });
}
function imageCacheKey(url){return String(url||"").trim()}
async function idbPutImage(url,blob){
  if(!url||!blob)return;
  const db=await idbOpen();
  return new Promise((resolve,reject)=>{
    const tx=db.transaction(FM_STORES.images,"readwrite");
    tx.objectStore(FM_STORES.images).put({url:imageCacheKey(url),blob,type:blob.type||"image/*",savedAt:Date.now()});
    tx.oncomplete=resolve;tx.onerror=()=>reject(tx.error);
  });
}
async function idbGetImage(url){
  if(!url)return null;
  const db=await idbOpen();
  return new Promise((resolve,reject)=>{
    const tx=db.transaction(FM_STORES.images,"readonly"),r=tx.objectStore(FM_STORES.images).get(imageCacheKey(url));
    r.onsuccess=()=>resolve(r.result||null);r.onerror=()=>reject(r.error);
  });
}
async function getCachedImageSrc(url){
  if(!url)return "";
  const key=imageCacheKey(url);
  if(fmImageObjectUrls.has(key))return fmImageObjectUrls.get(key);
  try{
    const cached=await idbGetImage(key);
    if(!cached?.blob)return url;
    const objectUrl=URL.createObjectURL(cached.blob);
    fmImageObjectUrls.set(key,objectUrl);
    return objectUrl;
  }catch(_){return url}
}
async function preloadImage(url){
  const key=imageCacheKey(url);if(!key)return;
  try{
    if(fmImageObjectUrls.has(key))return;
    const existing=await idbGetImage(key);
    if(existing?.blob){await getCachedImageSrc(key);return;}
    const response=await fetch(key,{mode:"cors",credentials:"omit",cache:"force-cache"});
    if(!response.ok)throw new Error(`image http ${response.status}`);
    const blob=await response.blob();
    await idbPutImage(key,blob);
    await getCachedImageSrc(key);
  }catch(_){
    // Some image hosts do not expose CORS. Fall back to the browser's normal image cache.
    try{const img=new Image();img.decoding="async";img.src=key;await img.decode?.();}catch(__){}
  }
}
async function preloadMessageImages(items){
  const urls=[...new Set((items||[]).flatMap(m=>Array.isArray(m?.imageUrls)?m.imageUrls:[]).filter(Boolean))];
  if(!urls.length)return;
  await Promise.allSettled(urls.slice(0,120).map(preloadImage));
}
async function hydrateRenderedMessageImages(items){
  if(!items?.length)return;
  const urls=[...new Set(items.flatMap(m=>Array.isArray(m?.imageUrls)?m.imageUrls:[]).filter(Boolean))];
  if(!urls.length)return;
  await Promise.allSettled(urls.map(async url=>{
    const src=await getCachedImageSrc(url);
    if(src===url)return;
    document.querySelectorAll(`.msg-img[data-image-url="${CSS.escape(url)}"]`).forEach(img=>{if(img.src!==src)img.src=src;});
  }));
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
  hydrateRenderedMessageImages(local).catch(()=>{});
  return local;
}
function renderMessagesFromPlain(items,options={}){
  const box=$("messages");if(!box)return;
  const shouldStickBottom=options.scrollToBottom!==false;
  if(!items.length){
    box.innerHTML='<div class="empty-state"><i class="fa-regular fa-comments"></i><b>No messages yet</b><span>Start the conversation.</span></div>';
    return;
  }
  box.innerHTML=items.map(m=>messageHTML(m)).join("");
  if(shouldStickBottom)box.scrollTop=box.scrollHeight;
}
async function loadOlderLocalMessages(){
  if(!currentConversationId||!oldestLoadedCreatedAt||syncInProgress)return;
  const box=$("messages");if(!box)return;
  const oldHeight=box.scrollHeight,oldTop=box.scrollTop;
  syncInProgress=true;
  try{
    let older=await idbMessages(currentConversationId,messagePageSize,oldestLoadedCreatedAt-1);
    if(!older.length){
      older=await fetchOlderConversationMessages(currentConversationId,oldestLoadedCreatedAt,3);
      if(older.length)await idbPutMessages(older);
    }
    if(!older.length){historyNoMore=true;return}
    older=older.map(m=>({...m,fmPrefetched:!!m.fmPrefetched}));
    const existing=new Set([...activeMessageMap.keys()]);
    older.forEach(m=>{activeMessageMap.set(m.id,m);messageMap.set(m.id,m)});
    const html=older.filter(m=>!existing.has(m.id)).map(m=>messageHTML(m)).join("");
    if(html){
      box.insertAdjacentHTML("afterbegin",html);
      oldestLoadedCreatedAt=older[0].createdAtMs;
      historyPage++;
      // Preserve the exact visual anchor: adding content above must not move the user's viewport.
      const delta=box.scrollHeight-oldHeight;
      box.scrollTop=oldTop+delta;
    }
    hydrateRenderedMessageImages(older).catch(()=>{});
    // When the user has reached the second page, quietly fetch the next three pages.
    if(historyPage>=2)prefetchNextHistoryPages().catch(e=>console.warn("history prefetch",e));
  }finally{syncInProgress=false}
}

async function fetchOlderConversationMessages(conversationId,beforeMs,pages=3){
  if(!me||!navigator.onLine)return [];
  const before=firebase.firestore.Timestamp.fromMillis(Number(beforeMs));
  const limit=Math.max(1,pages)*FM_PAGE_SIZE;
  const dedupe=new Map();
  const collect=async(q)=>{
    try{
      const snap=await q.orderBy("createdAt","desc").where("createdAt","<",before).limit(limit).get();
      snap.docs.forEach(d=>dedupe.set(d.id,normalizeLocalMessage({id:d.id,...d.data(),fmPrefetched:true})));
    }catch(orderErr){
      // Fallback keeps compatibility with existing Firestore indexes.
      try{
        const snap=await q.where("createdAt","<",before).get();
        snap.docs.forEach(d=>dedupe.set(d.id,normalizeLocalMessage({id:d.id,...d.data(),fmPrefetched:true})));
      }catch(fallbackErr){console.warn("older message query failed",fallbackErr)}
    }
  };
  if(conversationId.startsWith("group:")){
    await collect(MESSAGES().where("groupId","==",conversationId.slice(6)));
  }else{
    const [a,b]=conversationId.split("__");
    await Promise.all([
      collect(MESSAGES().where("senderUid","==",a).where("receiverUid","==",b)),
      collect(MESSAGES().where("senderUid","==",b).where("receiverUid","==",a))
    ]);
  }
  const items=[...dedupe.values()].sort((a,b)=>a.createdAtMs-b.createdAtMs).slice(-limit);
  if(items.length){await idbPutMessages(items);await preloadMessageImages(items)}
  return items;
}

async function prefetchNextHistoryPages(){
  if(historyPrefetchBusy||historyNoMore||!currentConversationId||!oldestLoadedCreatedAt||!navigator.onLine)return;
  historyPrefetchBusy=true;
  try{
    const items=await fetchOlderConversationMessages(currentConversationId,oldestLoadedCreatedAt,3);
    if(items.length){
      // Store as hidden historical cache; images are downloaded in the background but remain blurred in UI.
      items.forEach(m=>messageMap.set(m.id,m));
      const first=items[0]?.createdAtMs;
      if(first)await idbSetMeta("prefetchBefore_"+currentConversationId,first);
      if(items.length<FM_PAGE_SIZE*3)historyNoMore=true;
    }else historyNoMore=true;
  }finally{historyPrefetchBusy=false}
}

let fmImageZoom=1;
let fmViewerSourceUrl='';
let fmViewerPanzoom=null;
let fmViewerLastTap=0;

function destroyViewerPanzoom(){
  try{fmViewerPanzoom?.destroy?.()}catch(_){ }
  fmViewerPanzoom=null;
}

function createViewerPanzoom(){
  const stage=$('fmImageStage'), wrap=$('fmPanzoomWrap');
  if(!stage||!wrap||!window.Panzoom)return;
  destroyViewerPanzoom();
  fmViewerPanzoom=window.Panzoom(wrap,{
    maxScale:5,
    minScale:1,
    step:.22,
    contain:'inside',
    cursor:'grab',
    startScale:1,
    panOnlyWhenZoomed:true,
    animate:true,
    duration:190,
    easing:'ease-out',
    canvas:true,
    handleStartEvent:'pointerdown',
    excludeClass:'panzoom-exclude'
  });
  wrap.classList.remove('is-dragging');
  wrap.addEventListener('panzoomstart',()=>wrap.classList.add('is-dragging'));
  wrap.addEventListener('panzoomend',()=>wrap.classList.remove('is-dragging'));

  // Zoom around the exact mouse/pointer location. Panzoom expects focal
  // coordinates relative to its parent, so convert client coordinates to the
  // image stage's local coordinate system before every wheel zoom.
  stage.onwheel=e=>{
    if($('fmImageViewer')?.classList.contains('hidden') || !fmViewerPanzoom)return;
    e.preventDefault();e.stopPropagation();
    const rect=stage.getBoundingClientRect();
    const focal={x:e.clientX-rect.left,y:e.clientY-rect.top};
    const current=fmViewerPanzoom.getScale?.()||1;
    const factor=Math.exp(-e.deltaY*0.00115);
    const next=Math.max(1,Math.min(5,current*factor));
    if(Math.abs(next-current)<0.0001)return;
    try{
      fmViewerPanzoom.zoom(next,{focal,animate:true,duration:170});
    }catch(_){
      try{fmViewerPanzoom.zoom(next,{focal})}catch(__){}
    }
  };

  stage.ondblclick=e=>{
    e.preventDefault();e.stopPropagation();
    try{fmViewerPanzoom.reset({animate:true,duration:240})}catch(_){ }
  };
}

function ensureImageViewer(){
  if($('fmImageViewer'))return;
  const el=document.createElement('div');
  el.id='fmImageViewer';
  el.className='fm-image-viewer hidden';
  el.setAttribute('role','dialog');
  el.setAttribute('aria-modal','true');
  el.setAttribute('aria-label','Image viewer');
  el.innerHTML=`
    <div class="fm-image-viewer-backdrop"></div>
    <div class="fm-image-viewer-card">
      <button type="button" id="fmImageViewerClose" class="fm-image-viewer-close" aria-label="Close image viewer" title="Close">
        <i class="fa-solid fa-xmark" aria-hidden="true"></i>
      </button>
      <div class="fm-image-stage" id="fmImageStage">
        <div class="fm-panzoom-wrap" id="fmPanzoomWrap">
          <img id="fmViewerImage" alt="Image preview" draggable="false">
        </div>
        <div class="fm-image-viewer-hint">Wheel / pinch = zoom • drag = pan • double click = reset</div>
      </div>
    </div>`;
  document.body.appendChild(el);
  const img=$('fmViewerImage');
  const closeViewer=()=>closeImageViewer();
  $('fmImageViewerClose')?.addEventListener('click',e=>{e.preventDefault();e.stopPropagation();closeViewer();});
  el.querySelector('.fm-image-viewer-backdrop')?.addEventListener('click',closeViewer);
  el.addEventListener('keydown',e=>{if(e.key==='Escape')closeViewer();});
  el.tabIndex=-1;
  img.addEventListener('load',()=>{
    if(!$('fmImageViewer')||$('fmImageViewer').classList.contains('hidden'))return;
    requestAnimationFrame(()=>{
      createViewerPanzoom();
      try{fmViewerPanzoom?.reset?.({animate:false})}catch(_){ }
    });
  });
}

function showImage(url){
  if(!url)return;
  ensureImageViewer();
  fmViewerSourceUrl=url;
  const viewer=$('fmImageViewer'), img=$('fmViewerImage');
  const source=fmImageObjectUrls.get(url)||url;
  destroyViewerPanzoom();
  img.removeAttribute('style');
  img.src='';
  img.dataset.sourceUrl=url;
  viewer.classList.remove('hidden');
  document.body.classList.add('fm-viewer-open');
  viewer.focus({preventScroll:true});
  // Set the source only after the viewer is visible. Panzoom is created from
  // the loaded image dimensions, so every image gets its own correct bounds.
  requestAnimationFrame(()=>{img.src=source;});
}

function viewImg(url){showImage(url)}
function downloadViewerImage(){if(fmViewerSourceUrl)downloadOriginalImage(fmViewerSourceUrl)}
function closeImageViewer(){
  const el=$('fmImageViewer');if(!el)return;
  el.classList.add('hidden');
  destroyViewerPanzoom();
  const img=$('fmViewerImage');if(img)img.src='';
  fmViewerSourceUrl='';
  document.body.classList.remove('fm-viewer-open');
}

document.addEventListener('keydown',e=>{
  if(e.key==='Escape' && !$('fmImageViewer')?.classList.contains('hidden')) closeImageViewer();
});

async function downloadOriginalImage(url){
  if(!url)return;
  try{
    const cached=await idbGetImage(url);
    const blob=cached?.blob||(await fetch(url,{mode:"cors",credentials:"omit"})).blob();
    const b=await blob;
    const a=document.createElement("a");
    a.href=URL.createObjectURL(b);a.download=(url.split("/").pop()||"image").split("?")[0]||"image";
    document.body.appendChild(a);a.click();a.remove();
    setTimeout(()=>URL.revokeObjectURL(a.href),1000);
  }catch(e){window.open(url,"_blank","noopener,noreferrer")}
}

async function warmFriendChatCaches(){
  if(!me||!navigator.onLine||fmWarmupPromise)return fmWarmupPromise;
  fmWarmupPromise=(async()=>{
    try{
      await idbOpen();
      const friendIds=[...new Set(friends.map(f=>String(f?.friendUid||f?.uid||"")).filter(Boolean))];
      const groupIds=[...new Set(groups.map(g=>String(g?.id||g?.uid||"")).filter(Boolean))];
      const targets=[];
      friendIds.forEach(uid=>targets.push({kind:"friend",uid}));
      groupIds.forEach(uid=>targets.push({kind:"group",uid}));
      const concurrency=3;let cursor=0;
      const worker=async()=>{
        while(cursor<targets.length){
          const target=targets[cursor++];
          try{
            let docs=[];
            if(target.kind==="group"){
              const s=await MESSAGES().where("groupId","==",target.uid).get();
              docs=s.docs;
            }else{
              const [outgoing,incoming]=await Promise.all([
                MESSAGES().where("senderUid","==",me.uid).where("receiverUid","==",target.uid).get(),
                MESSAGES().where("senderUid","==",target.uid).where("receiverUid","==",me.uid).get()
              ]);
              const map=new Map();[...outgoing.docs,...incoming.docs].forEach(d=>map.set(d.id,d));docs=[...map.values()];
            }
            const items=docs.map(d=>normalizeLocalMessage({id:d.id,...d.data()})).sort((a,b)=>b.createdAtMs-a.createdAtMs).slice(0,FM_WARM_MESSAGE_LIMIT);
            if(items.length){
              await idbPutMessages(items);
              await preloadMessageImages(items);
              items.forEach(m=>messageMap.set(m.id,m));
            }
          }catch(e){console.warn("warm chat cache",target.uid,e)}
        }
      };
      await Promise.all(Array.from({length:Math.min(concurrency,Math.max(1,targets.length))},worker));
      cacheMessages();
      if(activeFriend)await renderLocalMessages(activeFriend.isGroup?`group:${activeFriend.uid}`:pair(me.uid,activeFriend.uid),FM_WARM_MESSAGE_LIMIT);
    }finally{fmWarmupPromise=null}
  })();
  return fmWarmupPromise;
}
function scheduleWarmFriendChatCaches(){
  if(!me||!navigator.onLine)return;
  const run=()=>warmFriendChatCaches().catch(e=>console.warn("warm chat cache",e));
  if(typeof requestIdleCallback==="function")requestIdleCallback(run,{timeout:1800});
  else setTimeout(run,250);
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
    heartbeat();startListeners();watchIncomingNotifications();watchCallInvites();scheduleWarmFriendChatCaches();
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
const composerInput=$("messageInput"),sendButton=$("composer .send-btn");
// A native button normally takes focus on touch. On mobile that can dismiss the
// virtual keyboard. Submit from pointerdown while keeping focus on the textarea.
if(sendButton&&composerInput){
  sendButton.addEventListener("pointerdown",e=>{
    if(e.pointerType==="touch"||e.pointerType==="pen"){
      if(sendButton.disabled)return;
      e.preventDefault();
      composerInput.focus({preventScroll:true});
      $("composer")?.requestSubmit?.(sendButton);
    }
  },{passive:false});
}
composerInput?.addEventListener("input",e=>{e.target.style.height="auto";e.target.style.height=Math.min(e.target.scrollHeight,120)+"px";handleTyping()});
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
initMessageReactions();

// Profile pictures open in the same full-screen lightbox.
["profileAvatar","userModalAvatar","chatAvatar","headerAvatar"].forEach(id=>{
  const el=$(id);
  if(el){
    el.style.cursor="zoom-in";
    el.addEventListener("click",e=>{
      e.preventDefault();
      e.stopPropagation();
      if(el.src) viewImg(el.src);
    });
  }
});

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
  // Home chat list must paint independently of the search box on first load.
  try{renderChats()}catch(e){console.warn("initial chat render",e)}
  try{renderReplyPreview()}catch(e){console.warn("initial reply preview",e)}
  try{await idbOpen()}catch(e){console.warn("IndexedDB unavailable",e)}
  updateConnectivity();
  const box=$("messages");
  if(box)box.addEventListener("scroll",()=>{
    if(box.scrollTop<140 && activeFriend&&!syncInProgress){
      const conversationId=activeFriend.isGroup?`group:${activeFriend.uid}`:pair(me.uid,activeFriend.uid);
      if(conversationId&&oldestLoadedCreatedAt)loadOlderLocalMessages();
    }
    // Do not block scrolling on network work; prefetch runs only after page 2 is reached.
    if(historyPage>=2 && box.scrollTop<Math.max(320,box.clientHeight*.75))prefetchNextHistoryPages().catch(()=>{});
  },{passive:true});
});


/* Smooth historical message loading: preserve scroll anchoring and avoid image layout shifts. */
(function installHistoryUX(){
  const style=document.createElement("style");style.id="fm-history-ux";style.textContent=`
    #messages{overflow-anchor:none;overscroll-behavior-y:contain;scroll-behavior:auto!important;}
    #messages .msg-img{contain:layout paint;min-height:24px;}
    #messages .fm-prefetched-image{filter:blur(16px);transform:translateZ(0);cursor:pointer;transition:filter .18s ease;}
    #messages .fm-prefetched-image:hover{filter:blur(12px);}
  `;document.head.appendChild(style);
})();

/* Camera preview controls */
document.addEventListener("click",async e=>{
  const id=e.target.closest("button")?.id;
  if(id==="closeCameraPreviewBtn"){closeCameraPreview();return}
  if(id==="previewMuteBtn"){togglePreviewMute();return}
  if(id==="previewFlipBtn"){await flipPreviewCamera();return}
  if(id==="previewLightBtn"){togglePreviewLight();return}
  if(id==="startVideoCallBtn"){if(pendingVideoCall){const mode=pendingVideoCall.mode;closeCameraPreview();pendingVideoCall=null;await launchCall(mode)}}
});
