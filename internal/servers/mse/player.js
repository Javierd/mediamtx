(function(){
const video = document.getElementById('video');
const message = document.getElementById('message');

const setMessage = (s)=>{ message.textContent = s||''; message.style.display = s? 'flex':'none'; };

function wsURL(){
  const loc = window.location;
  const proto = (loc.protocol === 'https:') ? 'wss' : 'ws';
  // path without trailing slash
  const path = loc.pathname.replace(/\/$/, '');
  return `${proto}://${loc.host}${path}`;
}

function detectCodec(init){
  // naive detection: attempt common combos; browsers will reject unsupported ones
  const candidates = [
    'video/mp4; codecs="avc1.64001f, mp4a.40.2"',
    'video/mp4; codecs="avc1.42E01E, mp4a.40.2"',
    'video/mp4; codecs="hvc1.1.6.L93.B0, mp4a.40.2"',
    'video/mp4; codecs="av01.0.08M.08, mp4a.40.2"',
    'video/mp4; codecs="vp09.00.10.08, mp4a.40.2"'
  ];
  for (const c of candidates){ if (MediaSource.isTypeSupported(c)) return c; }
  // fallback to a generic baseline
  return 'video/mp4; codecs="avc1.42E01E, mp4a.40.2"';
}

function start(){
  setMessage('connecting...');
  const ms = new MediaSource();
  video.src = URL.createObjectURL(ms);
  const sock = new WebSocket(wsURL());
  sock.binaryType = 'arraybuffer';

  let sb; // SourceBuffer
  let queue = [];
  let appending = false;

  function appendNext(){
    if (!sb || appending || queue.length === 0) return;
    appending = true;
    const data = queue.shift();
    try {
      sb.appendBuffer(data);
    } catch (e) {
      console.error('appendBuffer error', e);
      appending = false;
    }
  }

  function onUpdateEnd(){ appending = false; appendNext(); }

  ms.addEventListener('sourceopen', ()=>{
    setMessage('waiting for data...');
  });

  sock.onclose = ()=> setMessage('disconnected');
  sock.onerror = ()=> setMessage('connection error');

  sock.onmessage = (ev)=>{
    const data = new Uint8Array(ev.data);
    if (!sb){
      const mime = detectCodec(data);
      if (!MediaSource.isTypeSupported(mime)){
        setMessage('codec not supported by this browser');
        sock.close();
        return;
      }
      sb = ms.addSourceBuffer(mime);
      sb.mode = 'segments';
      sb.addEventListener('updateend', onUpdateEnd);
    }
    queue.push(data);
    appendNext();
    if (!video.autoplay) video.play().catch(()=>{});
    setMessage('');
  };
}

window.addEventListener('load', start);
})();