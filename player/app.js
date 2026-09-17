/* ================= IPTV Web Player (client-side only) =================
   يعتمد فقط على نقاط الخدمة الحالية: /player_api.php و /live/...
   لا يضيف أي حمل على السيرفر: فك الترميز يتم في المتصفح (hls.js)،
   ويُدمَّر المشغل عند إغلاق القناة لتتحرر عملية ffmpeg بعد انتهاء المهلة.
   يحترم حد الأجهزة (max_connections) — لا تجاوز.                        */

const $ = s => document.querySelector(s);
const enc = encodeURIComponent;

const DEVICE_ID = (() => {
  let d = localStorage.getItem('iptv_device_id');
  if (!d) { d = 'web-' + Math.random().toString(36).slice(2, 10) + Date.now().toString(36); localStorage.setItem('iptv_device_id', d); }
  return d;
})();

const S = {
  user: null, pass: null, userInfo: null,
  streams: [], categories: [], cat: 'all', search: '',
  favs: [], recents: [],
  current: null, currentUrl: '', art: null, retry: 0, rTimer: null,
  m3uMode: false
};

/* ---------- toast ---------- */
function notice(msg, type = 'ok') {
  const el = document.createElement('div');
  el.className = 'toast ' + type;
  el.innerHTML = '<span>' + (type === 'err' ? '⚠️' : '✓') + '</span><span>' + msg + '</span>';
  $('#toast-wrap').appendChild(el);
  setTimeout(() => { el.classList.add('out'); setTimeout(() => el.remove(), 260); }, 2600);
}

function setErr(msg) {
  const e = $('#login-err');
  e.textContent = msg; e.style.display = msg ? 'block' : 'none';
}

/* ---------- login tabs ---------- */
document.querySelectorAll('.tab').forEach(t => t.onclick = () => {
  document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
  t.classList.add('active');
  const m3u = t.dataset.mode === 'm3u';
  $('#xtream-form').classList.toggle('hidden', m3u);
  $('#m3u-form').classList.toggle('hidden', !m3u);
  setErr('');
});

/* ---------- xtream login ---------- */
$('#xtream-form').onsubmit = async (e) => {
  e.preventDefault();
  const u = $('#x-user').value.trim(), p = $('#x-pass').value.trim();
  const btn = $('#x-btn'); btn.disabled = true; btn.innerHTML = '<span class="spin"></span> جارِ الدخول...';
  setErr('');
  try {
    const r = await fetch(`/player_api.php?username=${enc(u)}&password=${enc(p)}&deviceId=${enc(DEVICE_ID)}`);
    const data = await r.json();
    if (!data.user_info || data.user_info.auth !== 1) throw new Error(data.user_info && data.user_info.message || 'بيانات الدخول غير صحيحة أو انتهى الحساب');
    S.user = u; S.pass = p; S.userInfo = data.user_info; S.m3uMode = false;
    if ($('#remember').checked) localStorage.setItem('iptv_creds', JSON.stringify({ u, p }));
    else localStorage.removeItem('iptv_creds');
    await loadXtream();
    enterApp();
  } catch (err) {
    setErr(err.message || 'تعذّر الاتصال بالسيرفر');
  }
  btn.disabled = false; btn.innerHTML = 'دخول';
};

async function loadXtream() {
  const q = `username=${enc(S.user)}&password=${enc(S.pass)}&deviceId=${enc(DEVICE_ID)}`;
  const [c, s] = await Promise.all([
    fetch(`/player_api.php?${q}&action=get_live_categories`).then(r => r.json()).catch(() => []),
    fetch(`/player_api.php?${q}&action=get_live_streams`).then(r => r.json()).catch(() => [])
  ]);
  S.categories = Array.isArray(c) ? c : [];
  const catName = {};
  S.categories.forEach(x => catName[x.category_id] = x.category_name);
  S.streams = (Array.isArray(s) ? s : []).map(x => ({
    id: x.stream_id,
    name: x.name || ('قناة ' + x.stream_id),
    group: catName[x.category_id] || 'أخرى',
    url: `/live/${enc(S.user)}/${enc(S.pass)}/${x.stream_id}.m3u8?deviceId=${enc(DEVICE_ID)}`
  }));
  loadPrefs();
}

/* ---------- m3u login ---------- */
$('#m3u-form').onsubmit = async (e) => {
  e.preventDefault();
  const url = $('#m-url').value.trim();
  const btn = $('#m-btn'); btn.disabled = true; btn.innerHTML = '<span class="spin"></span> جارِ التحميل...';
  setErr('');
  try {
    const txt = await fetch(url).then(r => { if (!r.ok) throw new Error('تعذّر تحميل الملف (' + r.status + ')'); return r.text(); });
    parseM3U(txt);
    if (!S.streams.length) throw new Error('لم يُعثر على قنوات في الملف أو أن المصدر يمنع الوصول (CORS)');
    S.m3uMode = true; S.user = 'm3u'; S.userInfo = null;
    if ($('#remember2').checked) localStorage.setItem('iptv_m3u_url', url);
    else localStorage.removeItem('iptv_m3u_url');
    loadPrefs();
    enterApp();
  } catch (err) {
    setErr(err.message || 'فشل تحميل قائمة التشغيل');
  }
  btn.disabled = false; btn.innerHTML = 'تحميل القنوات';
};

function parseM3U(txt) {
  const lines = txt.split(/\r?\n/);
  const out = [];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line.startsWith('#EXTINF')) continue;
    const name = (line.split(',').pop() || '').trim() || 'قناة';
    const logo = (line.match(/tvg-logo="([^"]*)"/) || [])[1] || '';
    const group = (line.match(/group-title="([^"]*)"/) || [])[1] || 'عام';
    let url = '';
    for (let j = i + 1; j < lines.length; j++) { if (lines[j].trim() && !lines[j].trim().startsWith('#')) { url = lines[j].trim(); break; } }
    if (url) out.push({ id: 'm' + out.length, name, group, url, icon: logo });
  }
  S.streams = out;
  const seen = {}, cats = [];
  out.forEach(s => { if (!seen[s.group]) { seen[s.group] = 1; cats.push({ category_id: s.group, category_name: s.group }); } });
  S.categories = cats;
}

/* ---------- prefs ---------- */
const favKey = () => 'iptv_fav_' + (S.user || 'guest');
const recKey = () => 'iptv_rec_' + (S.user || 'guest');
function loadPrefs() {
  try { S.favs = JSON.parse(localStorage.getItem(favKey()) || '[]'); } catch (e) { S.favs = []; }
  try { S.recents = JSON.parse(localStorage.getItem(recKey()) || '[]'); } catch (e) { S.recents = []; }
}
const saveFavs = () => localStorage.setItem(favKey(), JSON.stringify(S.favs));
const saveRecs = () => localStorage.setItem(recKey(), JSON.stringify(S.recents));

function pushRecent(stream) {
  const id = String(stream.id);
  S.recents = [id, ...S.recents.filter(x => x !== id)].slice(0, 30);
  saveRecs();
}

/* ---------- app ---------- */
function enterApp() {
  $('#login').classList.add('hidden');
  $('#app').classList.remove('hidden');
  $('#who').textContent = S.m3uMode ? 'قائمة M3U' : S.user;
  S.cat = 'all'; S.search = ''; $('#search').value = '';
  renderSidebar();
  renderGrid();
}
function logout() {
  destroyPlayer();
  localStorage.removeItem('iptv_creds');
  location.reload();
}
$('#logout').onclick = logout;
$('#menu-btn').onclick = () => $('#sidebar').classList.toggle('open');

/* ---------- sidebar ---------- */
function renderSidebar() {
  const counts = {};
  S.streams.forEach(s => counts[s.group] = (counts[s.group] || 0) + 1);
  $('#specialcats').innerHTML = `
    ${catRow('all', '📚', 'كل القنوات', S.streams.length, S.cat === 'all')}
    ${catRow('fav', '⭐', 'المفضلة', S.favs.length, S.cat === 'fav')}
    ${catRow('recent', '🕘', 'الأخيرة', S.recents.length, S.cat === 'recent')}`;
  $('#cats').innerHTML = S.categories.map(c =>
    catRow(c.category_name, '📡', c.category_name, counts[c.category_name] || 0, S.cat === c.category_name)
  ).join('') || '<div class="cat">لا مجموعات</div>';
  document.querySelectorAll('.cat[data-cat]').forEach(el => el.onclick = () => {
    S.cat = el.dataset.cat; $('#sidebar').classList.remove('open'); renderSidebar(); renderGrid();
  });
}
const catRow = (val, ico, label, n, active) => `
  <div class="cat ${active ? 'active' : ''}" data-cat="${esc(val)}"><span class="ico">${ico}</span><span>${esc(label)}</span><span class="cnt">${n}</span></div>`;

/* ---------- grid ---------- */
$('#search').oninput = e => { S.search = e.target.value; renderGrid(); };

function currentList() {
  let list = S.streams;
  if (S.cat === 'fav') list = S.streams.filter(s => S.favs.includes(String(s.id)));
  else if (S.cat === 'recent') list = S.recents.map(id => S.streams.find(s => String(s.id) === id)).filter(Boolean);
  else if (S.cat !== 'all') list = S.streams.filter(s => s.group === S.cat);
  const q = S.search.trim().toLowerCase();
  if (q) list = list.filter(s => (s.name || '').toLowerCase().includes(q) || (s.group || '').toLowerCase().includes(q));
  return list;
}

function renderGrid() {
  const title = S.cat === 'all' ? 'كل القنوات' : S.cat === 'fav' ? 'المفضلة' : S.cat === 'recent' ? 'الأخيرة' : S.cat;
  $('#list-title').textContent = title;
  const list = currentList();
  $('#list-count').textContent = list.length + ' قناة';
  if (!list.length) {
    $('#grid').innerHTML = `<div class="empty"><div class="big">📭</div>لا توجد قنوات</div>`;
    return;
  }
  $('#grid').innerHTML = list.map((s, i) => {
    const fav = S.favs.includes(String(s.id));
    return `<div class="ch" data-id="${esc(String(s.id))}" style="animation-delay:${Math.min(i * 0.015, .3)}s">
      <div class="play">▶</div>
      <div class="av" style="${avatarStyle(s.name)}">${initials(s.name)}</div>
      <div class="nm">${esc(s.name)}</div>
      <div class="badges"><span class="badge">${esc(s.group || 'عام')}</span></div>
      <div class="star ${fav ? 'on' : ''}" data-fav="${esc(String(s.id))}" title="مفضلة">${fav ? '★' : '☆'}</div>
    </div>`;
  }).join('');
  document.querySelectorAll('.ch').forEach(el => el.onclick = (ev) => {
    if (ev.target.dataset.fav !== undefined) return;
    const s = S.streams.find(x => String(x.id) === el.dataset.id);
    if (s) playStream(s);
  });
  document.querySelectorAll('.star').forEach(el => el.onclick = (ev) => {
    ev.stopPropagation();
    toggleFav(el.dataset.fav);
  });
}

function toggleFav(id) {
  id = String(id);
  if (S.favs.includes(id)) { S.favs = S.favs.filter(x => x !== id); notice('أُزيلت من المفضلة'); }
  else { S.favs = [id, ...S.favs]; notice('أُضيفت إلى المفضلة'); }
  saveFavs(); renderSidebar(); renderGrid();
  if (S.current && String(S.current.id) === id) updateFavBtn();
}

/* ---------- avatar helpers ---------- */
function hashHue(str) { let h = 0; for (let i = 0; i < str.length; i++) h = (h * 31 + str.charCodeAt(i)) % 360; return h; }
function avatarStyle(name) { const h = hashHue(name || '?'); return `background:linear-gradient(135deg,hsl(${h} 65% 48%),hsl(${(h + 40) % 360} 70% 38%))`; }
function initials(name) {
  const parts = (name || '?').trim().split(/\s+/).slice(0, 2);
  return parts.map(p => p[0] || '').join('').toUpperCase() || '?';
}
function esc(s) { return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])); }

/* ---------- player ---------- */
function playerVisible() { return !$('#player-view').classList.contains('hidden'); }

function playStream(stream) {
  S.current = stream;
  S.currentUrl = stream.url;
  S.retry = 0;
  pushRecent(stream);
  renderSidebar();
  $('#pv-name').textContent = stream.name;
  $('#pv-group').textContent = stream.group || '';
  $('#player-view').classList.remove('hidden');
  updateFavBtn();
  updateCons();
  startPlayer(stream.url);
}

function startPlayer(url) {
  destroyPlayer();
  setStatus('جارِ الاتصال...');
  $('#pv-quality').textContent = 'تلقائي';
  const i18n = window['artplayer-i18n-ar'] ? { ar: window['artplayer-i18n-ar'] } : undefined;
  const opts = {
    container: '#art',
    url,
    autoplay: true,
    autoSize: false,
    autoOrientation: true,
    fullscreen: true,
    fullscreenWeb: true,
    pip: true,
    setting: true,
    playbackRate: true,
    aspectRatio: true,
    flip: true,
    screenshot: true,
    hotkey: true,
    miniProgressBar: true,
    airplay: true,
    theme: '#6366f1',
    lang: 'ar',
    customType: {
      m3u8: function (video, u, art) { attachHls(video, u, art); }
    }
  };
  if (url.split('?')[0].toLowerCase().endsWith('.m3u8')) opts.type = 'm3u8';
  if (i18n) opts.i18n = i18n;
  if (window.artplayerPluginHlsControl) {
    opts.plugins = [window.artplayerPluginHlsControl({
      quality: { control: true, setting: true, title: 'الجودة', auto: 'تلقائي' },
      audio: { control: false, setting: true, title: 'الصوت', auto: 'تلقائي' }
    })];
  }
  try {
    S.art = new Artplayer(opts);
  } catch (e) {
    S.art = null; setStatus('تعذّر تشغيل المشغل'); return;
  }
  S.art.on('video:playing', () => { S.retry = 0; setStatus('يعمل'); });
  S.art.on('video:waiting', () => setStatus('جارِ التحميل...'));
  S.art.on('video:pause', () => setStatus('متوقف مؤقتاً'));
  S.art.on('video:ended', () => setStatus('انتهى'));
}

function attachHls(video, url, art) {
  if (window.Hls && Hls.isSupported()) {
    const hls = new Hls({
      lowLatencyMode: false,
      liveSyncDurationCount: 3,
      liveMaxLatencyDurationCount: 10,
      maxBufferLength: 30,
      maxMaxBufferLength: 60,
      backBufferLength: 30,
      manifestLoadingMaxRetry: 3,
      manifestLoadingRetryDelay: 1200,
      manifestLoadingMaxRetryTimeout: 8000,
      levelLoadingMaxRetry: 3,
      levelLoadingRetryDelay: 1200,
      fragLoadingMaxRetry: 3,
      fragLoadingRetryDelay: 1500
    });
    hls.loadSource(url);
    hls.attachMedia(video);
    art.hls = hls;
    hls.on(Hls.Events.LEVEL_SWITCHED, (e, d) => {
      const lvl = hls.levels && hls.levels[d.level];
      $('#pv-quality').textContent = lvl && lvl.height ? lvl.height + 'p' : 'تلقائي';
    });
    hls.on(Hls.Events.ERROR, (e, data) => onHlsError(data));
    art.on('destroy', () => { try { hls.destroy(); } catch (err) {} });
  } else if (video.canPlayType('application/vnd.apple.mpegurl')) {
    video.src = url;
    art.hls = null;
  } else {
    setStatus('المتصفح لا يدعم HLS');
    notice('متصفحك لا يدعم تشغيل HLS', 'err');
  }
}

function onHlsError(data) {
  if (!data || !data.fatal) return;
  const hls = S.art && S.art.hls;
  if (data.type === Hls.ErrorTypes.MEDIA_ERROR) {
    setStatus('إصلاح خطأ الوسائط...');
    try { hls.recoverMediaError(); } catch (e) { scheduleReload(); }
    return;
  }
  if (data.type === Hls.ErrorTypes.NETWORK_ERROR && data.details === 'manifestLoadError' && hls) {
    try { hls.startLoad(); return; } catch (e) {}
  }
  scheduleReload();
}

function scheduleReload() {
  S.retry++;
  if (S.retry > 8) { setStatus('تعذّر تشغيل القناة'); notice('تعذّر تشغيل القناة، جرّب قناة أخرى', 'err'); return; }
  setStatus('إعادة المحاولة... (' + S.retry + ')');
  clearTimeout(S.rTimer);
  S.rTimer = setTimeout(() => { if (playerVisible()) startPlayer(S.currentUrl); }, Math.min(1500 * S.retry, 8000));
}

function destroyPlayer() {
  clearTimeout(S.rTimer); S.rTimer = null;
  if (S.art) { try { S.art.destroy(false); } catch (e) {} S.art = null; }
  const box = $('#art'); if (box) box.innerHTML = '';
}

function closePlayer() {
  destroyPlayer();
  $('#player-view').classList.add('hidden');
  if (S.userInfo) fetchActiveCons();
}
$('#back').onclick = closePlayer;
$('#reload').onclick = () => { S.retry = 0; startPlayer(S.currentUrl); };

function updateFavBtn() {
  const on = S.current && S.favs.includes(String(S.current.id));
  $('#fav').classList.toggle('on', !!on);
  $('#fav').textContent = on ? '★' : '☆';
}
$('#fav').onclick = () => { if (S.current) toggleFav(S.current.id); };

function nextChannel(dir) {
  const list = S.streams;
  if (!list.length || !S.current) return;
  let i = list.findIndex(s => String(s.id) === String(S.current.id));
  if (i === -1) i = 0; else i = (i + dir + list.length) % list.length;
  playStream(list[i]);
}
$('#next').onclick = () => nextChannel(1);
$('#prev').onclick = () => nextChannel(-1);

document.addEventListener('keydown', e => {
  if (!playerVisible() || e.target.tagName === 'INPUT') return;
  if (e.key === 'n' || e.key === 'N') nextChannel(1);
  else if (e.key === 'p' || e.key === 'P') nextChannel(-1);
  else if (e.key === 'Escape') closePlayer();
});

/* ---------- status helpers ---------- */
function setStatus(t) { $('#pv-status').textContent = t; }
function updateCons() {
  if (!S.userInfo) { $('#pv-cons').textContent = '—'; return; }
  $('#pv-cons').textContent = (S.userInfo.active_cons || 0) + '/' + (S.userInfo.max_connections || 1);
}
async function fetchActiveCons() {
  if (S.m3uMode || !S.user) return;
  try {
    const r = await fetch(`/player_api.php?username=${enc(S.user)}&password=${enc(S.pass)}&deviceId=${enc(DEVICE_ID)}`);
    const d = await r.json();
    if (d.user_info && d.user_info.auth === 1) { S.userInfo = d.user_info; updateCons(); }
  } catch (e) {}
}

/* ---------- free server ffmpeg when leaving ---------- */
window.addEventListener('beforeunload', () => { destroyPlayer(); });

/* ---------- auto login ---------- */
(function autoLogin() {
  const saved = localStorage.getItem('iptv_creds');
  if (saved) {
    try {
      const c = JSON.parse(saved);
      if (c && c.u && c.p) {
        $('#x-user').value = c.u; $('#x-pass').value = c.p; $('#remember').checked = true;
        $('#xtream-form').dispatchEvent(new Event('submit'));
        return;
      }
    } catch (e) {}
  }
  const m3u = localStorage.getItem('iptv_m3u_url');
  if (m3u) {
    $('#m-url').value = m3u; $('#remember2').checked = true;
    document.querySelector('.tab[data-mode="m3u"]').click();
    $('#m3u-form').dispatchEvent(new Event('submit'));
  }
})();
