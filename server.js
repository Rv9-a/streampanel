const express = require('express');
const cors = require('cors');
const axios = require('axios');
const fs = require('fs');
const { spawn } = require('child_process');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const NodeMediaServer = require('node-media-server');

const app = express();
const PORT = 3000;

// --- تشغيل خادم RTMP لاستقبال البث من OBS ---
const nmsConfig = {
  rtmp: {
    port: 1935,
    chunk_size: 60000,
    gop_cache: true,
    ping: 30,
    ping_timeout: 60
  },
  http: {
    port: 8000,
    allow_origin: '*'
  }
};
const nms = new NodeMediaServer(nmsConfig);
nms.run();

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const db = new sqlite3.Database(path.join(__dirname, 'panel.db'), (err) => {
    if (err) console.error("Database Connection Error:", err.message);
});

const channelTypes = {};
const adminSessions = {};
const ffmpegProcesses = {};

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT,
        max_connections INTEGER DEFAULT 1,
        expire_date TEXT,
        status INTEGER DEFAULT 1
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS channels (
        id TEXT PRIMARY KEY,
        name TEXT,
        url TEXT,
        stream_type INTEGER DEFAULT 0,
        group_title TEXT DEFAULT 'سيرفر 1'
    )`);

    const defaultChannels = [
        ['bein1', 'beIN Sports 1 FHD', 'https://raw.githubusercontent.com/Ilias23-dev/S-AP/refs/heads/main/beIN1FHD.m3u8', 0, 'سيرفر 1'],
        ['bein2', 'beIN Sports 2 FHD', 'https://raw.githubusercontent.com/Ilias23-dev/S-AP/refs/heads/main/beIN2FHD.m3u8', 0, 'سيرفر 1'],
        ['bein3', 'beIN Sports 3 FHD', 'https://raw.githubusercontent.com/Ilias23-dev/S-AP/refs/heads/main/beIN3FHD.m3u8', 0, 'سيرفر 1'],
        ['bein4', 'beIN Sports 4 FHD', 'https://raw.githubusercontent.com/Ilias23-dev/S-AP/refs/heads/main/beIN4FHD.m3u8', 0, 'سيرفر 1'],
        ['bein5', 'beIN Sports 5 FHD', 'https://raw.githubusercontent.com/Ilias23-dev/S-AP/refs/heads/main/beIN5FHD.m3u8', 0, 'سيرفر 1'],
        ['bein6', 'beIN Sports 6 FHD', 'https://raw.githubusercontent.com/Ilias23-dev/S-AP/refs/heads/main/beIN6FHD.m3u8', 0, 'سيرفر 1'],
        ['bein7', 'beIN Sports 7 FHD', 'https://raw.githubusercontent.com/Ilias23-dev/S-AP/refs/heads/main/beIN7FHD.m3u8', 0, 'سيرفر 1'],
        ['rvtv_event', 'Rvtv (live event)', 'rtmp://127.0.0.1:1935/live/event', 0, 'سيرفر 1'],
        ['alwan1_4k', 'Alwan Sport 1 4K', 'http://185.191.126.127/live/cks43bj7qfsoa/y2w7etojwtdle/418111', 1, 'سيرفر 2'],
        ['alwan1_hd', 'Alwan Sport 1 HD', 'http://185.191.126.127/live/cks43bj7qfsoa/y2w7etojwtdle/418112', 1, 'سيرفر 2'],
        ['alwan2_4k', 'Alwan Sport 2 4K', 'http://185.191.126.127/live/cks43bj7qfsoa/y2w7etojwtdle/418114', 1, 'سيرفر 2'],
        ['alwan2_hd', 'Alwan Sport 2 HD', 'http://185.191.126.127/live/cks43bj7qfsoa/y2w7etojwtdle/418115', 1, 'سيرفر 2'],
        ['alwan3_4k', 'Alwan Sport 3 4K', 'http://185.191.126.127/live/cks43bj7qfsoa/y2w7etojwtdle/418117', 1, 'سيرفر 2'],
        ['alwan3_hd', 'Alwan Sport 3 HD', 'http://185.191.126.127/live/cks43bj7qfsoa/y2w7etojwtdle/418118', 1, 'سيرفر 2'],
        ['alwan4_4k', 'Alwan Sport 4 4K', 'http://185.191.126.127/live/cks43bj7qfsoa/y2w7etojwtdle/418120', 1, 'سيرفر 2'],
        ['alwan4_hd', 'Alwan Sport 4 HD', 'http://185.191.126.127/live/cks43bj7qfsoa/y2w7etojwtdle/418121', 1, 'سيرفر 2'],
        ['alwan5_4k', 'Alwan Sport 5 4K', 'http://185.191.126.127/live/cks43bj7qfsoa/y2w7etojwtdle/418123', 1, 'سيرفر 2'],
        ['alwan5_hd', 'Alwan Sport 5 HD', 'http://185.191.126.127/live/cks43bj7qfsoa/y2w7etojwtdle/418124', 1, 'سيرفر 2'],
        ['alwan6_4k', 'Alwan Sport 6 4K', 'http://185.191.126.127/live/cks43bj7qfsoa/y2w7etojwtdle/418126', 1, 'سيرفر 2']
    ];

    const stmt = db.prepare(`INSERT OR REPLACE INTO channels (id, name, url, stream_type, group_title) VALUES (?, ?, ?, ?, ?)`);
    defaultChannels.forEach(c => stmt.run(c));
    stmt.finalize();

    db.all(`SELECT * FROM channels`, [], (err, rows) => {
        if (!err && rows) {
            rows.forEach(ch => {
                channelTypes[ch.id] = ch.stream_type;
                if (ch.stream_type === 0) {
                    startChannelProcess(ch.id, ch.url, ch.stream_type);
                }
            });
        }
    });
});

app.use('/hls', express.static(__dirname));

const HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer': 'https://www.maziikaaaaaa.shop/',
    'Origin': 'https://www.maziikaaaaaa.shop'
};

app.get('/proxy-seg', async (req, res) => {
    let targetUrl = req.query.url;
    if (!targetUrl) return res.status(400).send('Missing URL');

    try {
        const response = await axios.get(targetUrl, {
            responseType: 'arraybuffer',
            headers: HEADERS,
            timeout: 10000
        });

        let buffer = Buffer.from(response.data);

        if (targetUrl.includes('.m3u8') || targetUrl.includes('.json')) {
            res.setHeader('Content-Type', 'application/x-mpegURL');
            let text = buffer.toString('utf8');
            let modified = text.split('\n').map(line => {
                let trimmed = line.trim();
                if (trimmed.startsWith('http')) {
                    return `http://127.0.0.1:${PORT}/proxy-seg?url=${encodeURIComponent(trimmed)}`;
                }
                return line;
            }).join('\n');
            return res.send(modified);
        }

        let cleanBuffer = buffer;
        if (buffer.length > 1280 && buffer[1280] === 0x47) {
            cleanBuffer = buffer.slice(1280);
        } else {
            let pos = 1280;
            for (let i = 0; i < Math.min(buffer.length, 3000); i++) {
                if (buffer[i] === 0x47 && (i + 188 < buffer.length) && buffer[i + 188] === 0x47) {
                    pos = i;
                    break;
                }
            }
            cleanBuffer = buffer.slice(pos);
        }

        res.setHeader('Content-Type', 'video/mp2t');
        res.setHeader('Content-Length', cleanBuffer.length);
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.send(cleanBuffer);

    } catch (err) {
        res.status(500).send("Proxy Error");
    }
});

function startChannelProcess(id, url, streamType = 0) {
    if (ffmpegProcesses[id]) return;

    const channelDir = path.join(__dirname, id);
    if (!fs.existsSync(channelDir)) {
        fs.mkdirSync(channelDir, { recursive: true });
    }

    const isRtmp = url.startsWith('rtmp://');
    const isDirect = parseInt(streamType) === 1;

    let inputSource = url;
    if (!isRtmp && !isDirect) {
        inputSource = `http://127.0.0.1:${PORT}/proxy-seg?url=${encodeURIComponent(url)}`;
    }

    const outputPath = path.join(channelDir, 'index.m3u8');

    let ffmpegArgs = ['-y', '-loglevel', 'error'];

    if (isDirect && !isRtmp) {
        ffmpegArgs.push(
            '-user_agent', HEADERS['User-Agent'],
            '-headers', `Referer: ${HEADERS['Referer']}\r\nOrigin: ${HEADERS['Origin']}\r\n`
        );
    }

    // زيادة البفر وإعدادات التقطيع لاستقرار أكثر وتفادي السرعة الزائدة
    const hlsTime = isRtmp ? '4' : '2';
    const hlsListSize = isRtmp ? '10' : '6';

    ffmpegArgs.push(
        '-rw_timeout', '10000000',
        '-analyzeduration', '10000000',
        '-probesize', '10000000',
        '-i', inputSource,
        '-c', 'copy',
        '-f', 'hls', 
        '-hls_time', hlsTime, 
        '-hls_list_size', hlsListSize, 
        '-hls_flags', 'delete_segments+append_list+omit_endlist',
        outputPath
    );

    const proc = spawn('ffmpeg', ffmpegArgs);
    ffmpegProcesses[id] = proc;

    proc.on('close', () => {
        delete ffmpegProcesses[id];
        if (channelTypes[id] === 0) {
            setTimeout(() => {
                db.get(`SELECT * FROM channels WHERE id = ?`, [id], (err, ch) => {
                    if (ch) startChannelProcess(ch.id, ch.url, ch.stream_type);
                });
            }, 3000);
        }
    });
}

app.get('/live/:username/:password/:channelId.m3u8', (req, res) => {
    const { username, password, channelId } = req.params;

    db.get(`SELECT * FROM users WHERE username = ? AND password = ? AND status = 1`, [username, password], (err, user) => {
        if (err || !user) return res.status(403).send('Unauthorized');
        if (new Date(user.expire_date) < new Date()) return res.status(403).send('Expired');

        db.get(`SELECT * FROM channels WHERE id = ?`, [channelId], (err, channel) => {
            if (!channel) return res.status(404).send('Not Found');

            const playlistPath = path.join(__dirname, channelId, 'index.m3u8');
            if (fs.existsSync(playlistPath)) {
                return res.redirect(`/hls/${channelId}/index.m3u8`);
            } else {
                res.status(503).send('Stream Building or Offline...');
            }
        });
    });
});

app.get('/playlist/:username/:password/get.m3u', (req, res) => {
    const { username, password } = req.params;
    const host = req.headers.host;

    db.get(`SELECT * FROM users WHERE username = ? AND password = ? AND status = 1`, [username, password], (err, user) => {
        if (err || !user) return res.status(403).send('Unauthorized');

        db.all(`SELECT * FROM channels`, [], (err, channels) => {
            let m3uContent = `#EXTM3U\n`;
            channels.forEach(ch => {
                const groupName = ch.group_title || 'سيرفر 1';
                m3uContent += `#EXTINF:-1 tvg-id="${ch.id}" tvg-name="${ch.name}" group-title="${groupName}",${ch.name}\n`;
                m3uContent += `http://${host}/live/${username}/${password}/${ch.id}.m3u8\n`;
            });

            res.setHeader('Content-Type', 'audio/x-mpegurl');
            res.send(m3uContent);
        });
    });
});

app.post('/api/login', (req, res) => {
    if (req.body.password === 'admin') {
        const token = Math.random().toString(36).substring(2);
        adminSessions[token] = true;
        return res.json({ success: true, token });
    }
    res.status(401).json({ success: false, error: 'كلمة المرور غير صحيحة' });
});

const checkAdmin = (req, res, next) => {
    const token = req.headers['x-admin-token'] || req.query.token;
    if (token && adminSessions[token]) return next();
    res.status(401).json({ error: 'Unauthorized' });
};

app.get('/api/channels', checkAdmin, (req, res) => {
    db.all(`SELECT * FROM channels`, [], (err, rows) => {
        res.json(rows.map(r => ({ ...r, running: !!ffmpegProcesses[r.id] })));
    });
});

app.post('/api/channels/add', checkAdmin, (req, res) => {
    const { id, name, url, stream_type, group_title } = req.body;
    const sType = parseInt(stream_type) || 0;
    
    db.run(`INSERT OR REPLACE INTO channels (id, name, url, stream_type, group_title) VALUES (?, ?, ?, ?, ?)`, 
        [id, name, url, sType, group_title || 'سيرفر 1'], 
        () => {
            channelTypes[id] = sType;
            if (sType === 0) {
                startChannelProcess(id, url, sType);
            }
            res.json({ success: true });
        });
});

app.post('/api/channels/delete', checkAdmin, (req, res) => {
    const { id } = req.body;
    if (ffmpegProcesses[id]) {
        ffmpegProcesses[id].kill('SIGKILL');
        delete ffmpegProcesses[id];
    }
    delete channelTypes[id];
    db.run(`DELETE FROM channels WHERE id = ?`, [id], () => res.json({ success: true }));
});

app.get('/api/users', checkAdmin, (req, res) => {
    db.all(`SELECT * FROM users`, [], (err, rows) => res.json(rows));
});

app.post('/api/users/add', checkAdmin, (req, res) => {
    const { username, password, max_connections, expire_date } = req.body;
    db.run(`INSERT INTO users (username, password, max_connections, expire_date) VALUES (?, ?, ?, ?)`,
        [username, password, max_connections || 1, expire_date],
        () => res.json({ success: true })
    );
});

app.post('/api/users/delete', checkAdmin, (req, res) => {
    db.run(`DELETE FROM users WHERE id = ?`, [req.body.id], () => res.json({ success: true }));
});

const adminHtml = `<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <title>لوحة IPTV - الإدارة</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.rtl.min.css" rel="stylesheet">
    <style>
        body { background: #0f172a; color: #f8fafc; font-family: system-ui; }
        .card { background: #1e293b; border: 1px solid #334155; color: #fff; }
    </style>
</head>
<body>
    <div id="login-container" class="container d-flex justify-content-center align-items-center vh-100">
        <div class="card p-4" style="width: 350px;">
            <h3 class="text-center mb-4">🔐 تسجيل دخول اللوحة</h3>
            <div id="login-error" class="alert alert-danger d-none py-2 text-center"></div>
            <form id="login-form">
                <div class="mb-3">
                    <label class="form-label">كلمة مرور اللوحة</label>
                    <input type="password" id="admin_password" class="form-control" placeholder="أدخل كلمة المرور (admin)" required>
                </div>
                <button type="submit" class="btn btn-primary w-100">دخول</button>
            </form>
        </div>
    </div>

    <div id="panel-container" class="container-fluid p-4 d-none">
        <div class="d-flex justify-content-between align-items-center mb-4">
            <h2>⚡ لوحة إدارة البث والسيرفرات</h2>
            <button class="btn btn-outline-danger btn-sm" onclick="logout()">تسجيل الخروج</button>
        </div>
        <div class="row g-4">
            <div class="col-lg-6">
                <div class="card p-3 mb-4">
                    <h4>➕ إضافة قناة لـ سيرفر</h4>
                    <hr>
                    <form id="channel-form" class="row g-2">
                        <div class="col-md-6"><input type="text" id="ch_id" class="form-control" placeholder="معرّف (rvtv_event)" required></div>
                        <div class="col-md-6"><input type="text" id="ch_name" class="form-control" placeholder="اسم القناة" required></div>
                        <div class="col-12"><input type="text" id="ch_group" class="form-control" placeholder="اسم السيرفر" value="سيرفر 1" required></div>
                        <div class="col-12"><input type="url" id="ch_url" class="form-control" placeholder="رابط Stream الأصلي أو RTMP" required></div>
                        <div class="col-12">
                            <select id="ch_stream_type" class="form-select">
                                <option value="0" selected>🔄 يعمل دائماً بالخلفية</option>
                                <option value="1">📡 عند الطلب On-Demand</option>
                            </select>
                        </div>
                        <button type="submit" class="btn btn-primary w-100 mt-2">إضافة القناة</button>
                    </form>
                </div>
                <div class="card p-3">
                    <h4>📺 القنوات المضافة</h4>
                    <hr>
                    <div id="channels-list"></div>
                </div>
            </div>
            <div class="col-lg-6">
                <div class="card p-3">
                    <h4>👤 إضافة مشترك جديد</h4>
                    <hr>
                    <form id="user-form" class="row g-2">
                        <div class="col-6"><input type="text" id="username" class="form-control" placeholder="اسم المستخدم" required></div>
                        <div class="col-6"><input type="text" id="password" class="form-control" placeholder="كلمة السر" required></div>
                        <div class="col-6"><input type="number" id="max_conn" class="form-control" value="1" placeholder="عدد الأجهزة" required></div>
                        <div class="col-6"><input type="date" id="expire_date" class="form-control" required></div>
                        <button type="submit" class="btn btn-success w-100 mt-2">إضافة المشترك</button>
                    </form>
                    <h4 class="mt-4">📋 قائمة المشتركين وروابط M3U</h4>
                    <hr>
                    <div id="users-list"></div>
                </div>
            </div>
        </div>
    </div>
    <script>
        const HOST = window.location.host;
        let token = localStorage.getItem('admin_token');
        if (token) showPanel();

        document.getElementById('login-form').onsubmit = async (e) => {
            e.preventDefault();
            const password = document.getElementById('admin_password').value;
            const res = await fetch('/api/login', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ password })
            });
            const data = await res.json();
            if (data.success) {
                token = data.token;
                localStorage.setItem('admin_token', token);
                showPanel();
            } else {
                const errDiv = document.getElementById('login-error');
                errDiv.innerText = data.error;
                errDiv.classList.remove('d-none');
            }
        };

        function logout() {
            localStorage.removeItem('admin_token');
            location.reload();
        }

        function showPanel() {
            document.getElementById('login-container').classList.add('d-none');
            document.getElementById('panel-container').classList.remove('d-none');
            loadChannels();
            loadUsers();
        }

        async function fetchWithAuth(url, options = {}) {
            options.headers = options.headers || {};
            options.headers['x-admin-token'] = token;
            const res = await fetch(url, options);
            if (res.status === 401) logout();
            return res;
        }

        async function loadChannels() {
            const res = await fetchWithAuth('/api/channels');
            const channels = await res.json();
            document.getElementById('channels-list').innerHTML = channels.map(c => \`
                <div class="d-flex justify-content-between align-items-center mb-2 p-2 bg-dark rounded">
                    <div>
                        <strong>\${c.name}</strong> (\${c.id}) 
                        <span class="badge bg-primary">\${c.group_title}</span>
                        <span class="badge \${c.running ? 'bg-success' : 'bg-secondary'}">\${c.running ? '🟢 تعمل' : '⚪ خاملة'}</span>
                    </div>
                    <button class="btn btn-sm btn-outline-danger" onclick="deleteChannel('\${c.id}')">حذف</button>
                </div>
            \`).join('');
        }

        async function deleteChannel(id) {
            if(!confirm('متأكد من حذف القناة؟')) return;
            await fetchWithAuth('/api/channels/delete', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({id})
            });
            loadChannels();
        }

        document.getElementById('channel-form').onsubmit = async (e) => {
            e.preventDefault();
            await fetchWithAuth('/api/channels/add', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    id: document.getElementById('ch_id').value,
                    name: document.getElementById('ch_name').value,
                    group_title: document.getElementById('ch_group').value,
                    url: document.getElementById('ch_url').value,
                    stream_type: document.getElementById('ch_stream_type').value
                })
            });
            e.target.reset();
            loadChannels();
        };

        async function loadUsers() {
            const res = await fetchWithAuth('/api/users');
            const users = await res.json();
            document.getElementById('users-list').innerHTML = users.map(u => {
                const m3uUrl = \`http://\${HOST}/playlist/\${u.username}/\${u.password}/get.m3u\`;
                return \`
                <div class="p-2 mb-2 bg-dark rounded small">
                    <div class="d-flex justify-content-between align-items-center mb-1">
                        <span>👤 <b>\${u.username}</b> | 🔑 \${u.password} | 📅 \${u.expire_date}</span>
                        <button class="btn btn-sm btn-danger py-0" onclick="deleteUser(\${u.id})">حذف</button>
                    </div>
                    <div class="input-group input-group-sm">
                        <input type="text" class="form-control" value="\${m3uUrl}" readonly>
                        <button class="btn btn-outline-info" onclick="navigator.clipboard.writeText('\${m3uUrl}'); alert('تم النسخ!');">📋 نسخ</button>
                    </div>
                </div>
                \`;
            }).join('');
        }

        document.getElementById('user-form').onsubmit = async (e) => {
            e.preventDefault();
            await fetchWithAuth('/api/users/add', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    username: document.getElementById('username').value,
                    password: document.getElementById('password').value,
                    max_connections: document.getElementById('max_conn').value,
                    expire_date: document.getElementById('expire_date').value
                })
            });
            e.target.reset();
            loadUsers();
        };

        async function deleteUser(id) {
            if(!confirm('متأكد من حذف المشترك؟')) return;
            await fetchWithAuth('/api/users/delete', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({id})
            });
            loadUsers();
        }
    </script>
</body>
</html>`;

app.get('/admin', (req, res) => {
    res.send(adminHtml);
});

app.listen(PORT, () => console.log(`IPTV Panel running on port ${PORT}`));
