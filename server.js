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
const channelRetries = {};
const channelAlwaysOn = {};
const channelLastAccess = {};
const MAX_CHANNEL_RETRIES = 5;
const IDLE_TIMEOUT_MS = 120000;
const IDLE_CHECK_MS = 30000;

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
        group_title TEXT DEFAULT 'سيرفر 1',
        always_on INTEGER DEFAULT 0
    )`);

    db.run(`ALTER TABLE channels ADD COLUMN always_on INTEGER DEFAULT 0`, (migErr) => {
        if (!migErr) {
            console.log('[DB] migrated: added always_on column');
        } else if (migErr.message && migErr.message.includes('duplicate column')) {
            // العمود موجود أصلاً — قاعدة بيانات أخرى أو جدول جديد يشمل العمود
        } else {
            console.error('[DB] migration check:', migErr.message);
        }

        const stmt = db.prepare(`INSERT OR REPLACE INTO channels (id, name, url, stream_type, group_title, always_on) VALUES (?, ?, ?, ?, ?, ?)`);
        stmt.on('error', (e) => {
            console.error(`[DB] channel insert error:`, e.message);
        });
        defaultChannels.forEach(c => {
            stmt.run(c, (e) => {
                if (e) console.error(`[DB] insert error for ${c[0]}:`, e.message);
            });
        });
        stmt.finalize();

        console.log(`[DB] default channels ready: ${defaultChannels.length} total`);

        db.run(`DELETE FROM channels WHERE id LIKE 'besp%'
                OR id IN ('4k')
                OR (id LIKE 'alwan%' AND id NOT LIKE 'alwan_hd%' AND id NOT LIKE 'alwan_4k%')
                OR id IN ('bein1_4k', 'bein2_4k', 'bein3_4k', 'bein4_4k', 'bein5_4k', 'bein6_4k', 'bein7_4k', 'bein8_4k', 'bein9_4k')`, (cleanErr) => {
            if (cleanErr) console.error('[DB] cleanup error:', cleanErr.message);
        });

        db.all(`SELECT * FROM channels`, [], (err, rows) => {
            if (!err && rows) {
                const alwaysChannels = rows.filter(r => r.always_on);
                if (alwaysChannels.length) console.log(`[DB] starting ${alwaysChannels.length} always-on channels at boot`);
                rows.forEach(ch => {
                    channelTypes[ch.id] = ch.stream_type;
                    channelAlwaysOn[ch.id] = !!ch.always_on;
                    if (ch.always_on) {
                        startChannelProcess(ch.id, ch.url, ch.stream_type, true);
                    }
                });
            }
        });
    });
});

    const groupRV = 'BEIN RV';
    const groupSS = 'bein sport ss';
    const groupSrc = 'bein sport مصدر خاص';
    const groupAlK = 'ALKASS الكأس';
    const groupAlwan = 'ALWAN SPORT';

    const defaultChannels = [];
    const mk = (id, name, url, group, always = 0, streamType = 1) => [id, name, url, streamType, group, always];

    for (let i = 1; i <= 7; i++) {
        defaultChannels.push(mk(`bein${i}`, `beIN Sports ${i} FHD`, `https://raw.githubusercontent.com/Ilias23-dev/S-AP/refs/heads/main/beIN${i}FHD.m3u8`, groupRV, 1, 0));
    }
    defaultChannels.push(mk('rvtv_event', 'Rvtv (live event)', 'rtmp://127.0.0.1:1935/live/event', groupRV, 1, 0));

    const ssBase = 'http://pro.netmos.ovh:7355/live/EXMOQNS9Y30998CX0/LKHSB87278DOKCPP/';
    defaultChannels.push(mk('bein_ss_4k_true', 'bein 4K (true 4k)', `${ssBase}158960.ts`, `${groupSS}/4K`));
    defaultChannels.push(mk('bein_ss_4k_event', 'bein 4K (only event)', `${ssBase}158961.ts`, `${groupSS}/4K`));
    defaultChannels.push(mk('bein_ss_news', 'bein news', `${ssBase}83618.ts`, `${groupSS}/متنوعة`));
    defaultChannels.push(mk('bein_ss_global', 'bein global', `${ssBase}231675.ts`, `${groupSS}/متنوعة`));

    const ssSd = ['102890', '102891', '108484', '108485', '158897', '102895', '108486', '158898', '158899'];
    ssSd.forEach((u, i) => defaultChannels.push(mk(`bein_ss_sd${i + 1}`, `beinsport ${i + 1} SD`, `${ssBase}${u}.ts`, `${groupSS}/SD`)));
    const ssHd = ['158866', '158889', '158890', '158891', '158892', '158893', '158894', '158895', '158896'];
    ssHd.forEach((u, i) => defaultChannels.push(mk(`bein_ss_hd${i + 1}`, `beinsport ${i + 1} HD`, `${ssBase}${u}.ts`, `${groupSS}/HD`)));
    const ssFhd = ['158867', '158900', '158901', '158902', '158903', '158904', '158905', '158906', '158907'];
    ssFhd.forEach((u, i) => defaultChannels.push(mk(`bein_ss_fhd${i + 1}`, `beinsport ${i + 1} FHD`, `${ssBase}${u}.ts`, `${groupSS}/FHD`)));
    const ss4k = ['221764', '221765', '221766', '221767', 'https://prime-fast.sytes.net/prime-tv/stream/78.m3u8', '221769', '221770', '221771'];
    ss4k.forEach((u, i) => {
        const url = u.startsWith('http') ? u : `${ssBase}${u}.ts`;
        defaultChannels.push(mk(`bein_ss_4k${i + 1}`, `beinsport 4K ${i + 1} FHD`, url, `${groupSS}/4K`));
    });

    const pfBase = 'https://prime-fast.sytes.net/prime-tv/stream/';
    for (let i = 1; i <= 9; i++) defaultChannels.push(mk(`bein_src4k${i}`, `bein ${i} 4K`, `${pfBase}${73 + i}.m3u8`, `${groupSrc}/4K`));
    for (let i = 1; i <= 9; i++) defaultChannels.push(mk(`bein_srcuhd${i}`, `bein sports ${i} (UHD)`, `http://fackyou-cdn5.cfd/BEIN-${i}/index.m3u8`, `${groupSrc}/UHD`));
    for (let i = 1; i <= 9; i++) defaultChannels.push(mk(`bein_srcfhd${i}`, `bein sports ${i} FHD`, `${pfBase}${62 + i}.m3u8`, `${groupSrc}/FHD`));
    for (let i = 1; i <= 9; i++) defaultChannels.push(mk(`bein_srchd${i}`, `bein sports ${i} HD`, `${pfBase}${51 + i}.m3u8`, `${groupSrc}/HD`));
    for (let i = 1; i <= 9; i++) defaultChannels.push(mk(`bein_srcsd${i}`, `bein sports ${i} SD`, `${pfBase}${24 + i}.m3u8`, `${groupSrc}/SD`));
    for (let i = 1; i <= 9; i++) defaultChannels.push(mk(`bein_srcmob${i}`, `bein sports ${i}`, `http://82.39.115.26:3000/live/${i}.m3u8`, `${groupSrc}/وقت المباريات`));

    const kWords = ['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight'];
    for (let i = 0; i < 8; i++) defaultChannels.push(mk(`alkass${i + 1}`, `الكأس ${i + 1}`, `https://alkass.kianezidi.workers.dev/${kWords[i]}.m3u8`, groupAlK));

    const alwanHd = ['232595', '232596', '232597', '232598', '232599', '232600'];
    alwanHd.forEach((u, i) => defaultChannels.push(mk(`alwan_hd${i + 1}`, `ALWAN SPORT ${i + 1} HD`, `${ssBase}${u}.ts`, `${groupAlwan}/HD`)));
    const alwan4k = ['232601', '232602', '232603', '232604', '232605', '232606'];
    alwan4k.forEach((u, i) => defaultChannels.push(mk(`alwan_4k${i + 1}`, `ALWAN SPORT ${i + 1} 4K`, `${ssBase}${u}.ts`, `${groupAlwan}/4K`)));

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

function startChannelProcess(id, url, streamType = 0, alwaysOn = false) {
    if (ffmpegProcesses[id]) return;

    channelAlwaysOn[id] = alwaysOn;
    if (!alwaysOn) console.log(`[On-Demand start - ${id}]: starting ffmpeg`);

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
        ffmpegArgs.push('-user_agent', HEADERS['User-Agent']);
    } else if (!isRtmp) {
        ffmpegArgs.push(
            '-user_agent', HEADERS['User-Agent'],
            '-headers', `Referer: ${HEADERS['Referer']}\r\nOrigin: ${HEADERS['Origin']}\r\n`
        );
    }

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

    let proc;
    try {
        proc = spawn('ffmpeg', ffmpegArgs);
    } catch (err) {
        console.error(`[spawn error - ${id}]: ${err.message}`);
        scheduleChannelRetry(id);
        return;
    }

    ffmpegProcesses[id] = proc;

    proc.stderr.on('data', (data) => {
        const msg = data.toString().trim();
        if (msg) console.log(`[FFmpeg ${id}]: ${msg}`);
    });

    proc.on('error', (err) => {
        console.error(`[FFmpeg error - ${id}]: ${err.message}`);
        delete ffmpegProcesses[id];
        scheduleChannelRetry(id, isRtmp);
    });

    proc.on('close', (code) => {
        delete ffmpegProcesses[id];
        if (code !== 0) {
            console.error(`[FFmpeg exit - ${id}]: code ${code}`);
            scheduleChannelRetry(id, isRtmp);
        } else {
            console.log(`[FFmpeg exit - ${id}]: normal exit`);
        }
    });
}

function scheduleChannelRetry(id, isRtmp = false) {
    if (ffmpegProcesses[id]) return;

    const alwaysOn = !!channelAlwaysOn[id];

    if (!alwaysOn && Date.now() - (channelLastAccess[id] || 0) > 15000) {
        channelRetries[id] = 0;
        console.log(`[FFmpeg idle - ${id}]: on-demand with no active viewers, staying stopped`);
        return;
    }

    const attempt = channelRetries[id] || 0;

    if (isRtmp) {
        channelRetries[id] = attempt + 1;
        const delay = 10000;
        console.log(`[FFmpeg retry - ${id}]: RTMP waiting for source, retry in ${delay / 1000}s`);
        setTimeout(() => {
            db.get(`SELECT * FROM channels WHERE id = ?`, [id], (err, ch) => {
                if (ch && !ffmpegProcesses[id]) startChannelProcess(ch.id, ch.url, ch.stream_type, !!channelAlwaysOn[id]);
            });
        }, delay);
        return;
    }

    const maxRetries = alwaysOn ? MAX_CHANNEL_RETRIES : 3;
    if (attempt >= maxRetries) {
        console.error(`[FFmpeg stop - ${id}]: max retries reached, stopping`);
        channelRetries[id] = 0;
        return;
    }
    channelRetries[id] = attempt + 1;
    const delays = alwaysOn ? [3000, 15000, 30000, 60000, 120000] : [3000, 6000, 12000];
    const delay = delays[Math.min(attempt, delays.length - 1)];
    console.log(`[FFmpeg retry - ${id}]: attempt ${attempt + 1}/${maxRetries} in ${delay / 1000}s`);
    setTimeout(() => {
        db.get(`SELECT * FROM channels WHERE id = ?`, [id], (err, ch) => {
            if (ch && !ffmpegProcesses[id]) {
                channelRetries[id] = 0;
                startChannelProcess(ch.id, ch.url, ch.stream_type, !!channelAlwaysOn[ch.id]);
            }
        });
    }, delay);
}

function cleanChannelDir(id) {
    const dir = path.join(__dirname, id);
    if (!fs.existsSync(dir)) return;
    try {
        fs.readdirSync(dir).forEach(f => {
            if (f.endsWith('.ts') || f === 'index.m3u8') {
                fs.unlinkSync(path.join(dir, f));
            }
        });
    } catch (err) {
        console.error(`[cleanDir - ${id}]: ${err.message}`);
    }
}

function checkIdleChannels() {
    const now = Date.now();
    for (const id of Object.keys(ffmpegProcesses)) {
        if (channelAlwaysOn[id]) continue;
        const last = channelLastAccess[id] || 0;
        if (now - last > IDLE_TIMEOUT_MS) {
            console.log(`[On-Demand stop - ${id}]: idle ${Math.round((now - last) / 1000)}s, stopping ffmpeg`);
            try { ffmpegProcesses[id].kill('SIGKILL'); } catch (e) { /* already gone */ }
            delete ffmpegProcesses[id];
            delete channelRetries[id];
            cleanChannelDir(id);
        }
    }
}
setInterval(checkIdleChannels, IDLE_CHECK_MS);

app.get('/live/:username/:password/:channelId.m3u8', (req, res) => {
    const { username, password, channelId } = req.params;

    db.get(`SELECT * FROM users WHERE username = ? AND password = ? AND status = 1`, [username, password], (err, user) => {
        if (err || !user) return res.status(403).send('Unauthorized');
        if (new Date(user.expire_date) < new Date()) return res.status(403).send('Expired');

        db.get(`SELECT * FROM channels WHERE id = ?`, [channelId], (err, channel) => {
            if (!channel) return res.status(404).send('Not Found');

            channelLastAccess[channelId] = Date.now();
            const alwaysOn = !!channelAlwaysOn[channelId];

            if (!ffmpegProcesses[channelId]) {
                if (!alwaysOn) cleanChannelDir(channelId);
                startChannelProcess(channelId, channel.url, channel.stream_type, alwaysOn);
            }

            const playlistPath = path.join(__dirname, channelId, 'index.m3u8');
            const startedAt = Date.now();

            const pollPlaylist = () => {
                if (fs.existsSync(playlistPath)) {
                    channelLastAccess[channelId] = Date.now();
                    return res.redirect(`/hls/${channelId}/index.m3u8`);
                }
                if (!ffmpegProcesses[channelId]) {
                    return res.status(503).send('Stream offline (source unavailable)');
                }
                if (Date.now() - startedAt > 15000) {
                    return res.status(503).send('Stream is still building, try again...');
                }
                setTimeout(pollPlaylist, 1000);
            };
            pollPlaylist();
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
    const alwaysOn = req.body.always_on ? 1 : 0;
    
    db.run(`INSERT OR REPLACE INTO channels (id, name, url, stream_type, group_title, always_on) VALUES (?, ?, ?, ?, ?, ?)`, 
        [id, name, url, sType, group_title || 'سيرفر 1', alwaysOn], 
        () => {
            channelTypes[id] = sType;
            channelAlwaysOn[id] = !!alwaysOn;
            if (alwaysOn) startChannelProcess(id, url, sType, true);
            res.json({ success: true });
        });
});

app.post('/api/channels/delete', checkAdmin, (req, res) => {
    const { id } = req.body;
    if (ffmpegProcesses[id]) {
        try { ffmpegProcesses[id].kill('SIGKILL'); } catch (e) {}
        delete ffmpegProcesses[id];
    }
    delete channelTypes[id];
    delete channelAlwaysOn[id];
    delete channelLastAccess[id];
    delete channelRetries[id];
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
                        <div class="col-12"><input type="text" id="ch_group" class="form-control" placeholder="المجموعة (استخدم / للتصنيف)" value="BEIN RV" required></div>
                        <div class="col-12"><input type="url" id="ch_url" class="form-control" placeholder="رابط Stream الأصلي أو RTMP" required></div>
                        <div class="col-12">
                            <select id="ch_stream_type" class="form-select">
                                <option value="0" selected>🔒 مصدر محمي (يحتاج بروكسي)</option>
                                <option value="1">📡 رابط مباشر (بدون بروكسي)</option>
                            </select>
                        </div>
                        <div class="col-12">
                            <select id="ch_always" class="form-select">
                                <option value="0" selected>⏸ عند الطلب فقط (يبدأ عند أول مشاهد)</option>
                                <option value="1">🔁 يعمل دائماً بالخلفية</option>
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
                        <span class="badge \${c.always_on ? 'bg-warning text-dark' : 'bg-info'}">\${c.always_on ? '🔁 دائماً' : '⏸ عند الطلب'}</span>
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
                    stream_type: document.getElementById('ch_stream_type').value,
                    always_on: document.getElementById('ch_always').value === '1'
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

process.on('uncaughtException', (err) => {
    console.error('UNCAUGHT EXCEPTION (panel continues):', err.message);
});

process.on('unhandledRejection', (reason) => {
    console.error('UNHANDLED REJECTION (panel continues):', reason);
});

app.listen(PORT, () => console.log(`IPTV Panel running on port ${PORT}`));
app.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
        console.error(`Port ${PORT} already in use — restarting manually required`);
    } else {
        console.error('Panel server error:', err.message);
    }
});
