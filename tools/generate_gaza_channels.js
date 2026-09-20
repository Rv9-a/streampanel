'use strict';
const fs = require('fs');
const path = require('path');

const INPUT = process.argv[2] || 'C:\\Users\\mmmoh\\Downloads\\Telegram Desktop\\ابن غزة الجوكر.m3u';
const OUTPUT = path.join(__dirname, '..', 'gaza_channels.js');

const raw = fs.readFileSync(INPUT, 'utf8');
const lines = raw.split(/\r?\n/);

const entries = [];
let cur = null;
for (const line of lines) {
    if (line.startsWith('#EXTINF')) {
        const m = /group-title="[^"]*",\s*(.*)$/.exec(line);
        cur = { name: (m ? m[1] : '').trim(), url: '', ua: '', ref: '' };
        entries.push(cur);
    } else if (cur) {
        let m;
        if ((m = /^#EXTVLCOPT:http-user-agent=(.*)$/.exec(line))) cur.ua = m[1].trim();
        else if ((m = /^#EXTVLCOPT:http-referrer=(.*)$/.exec(line))) cur.ref = m[1].trim();
        else if (/^http/i.test(line) && !cur.url) cur.url = line.trim();
    }
}

function classify(name) {
    const u = name.toUpperCase();
    if (u.startsWith('FR:')) {
        if (/RMC SPORT|CANAL\+|BEIN|EUROSPORT|AF CANAL/.test(u)) {
            if (/HEVC/.test(u)) return 'FR: Sports (HEVC)';
            return 'FR: Sports';
        }
        if (/HEVC/.test(u)) return 'FR: HEVC';
        return 'FR: Cine/TV';
    }
    if (/^AR[: ]/.test(u)) return 'AR: Movies';
    if (/^IL[: ]/.test(u) || /^SR[: ]/.test(u)) return 'HBO';
    if (/^IR[: ]/.test(u)) return 'MBC';
    if (/^IN[: ]/.test(u)) return 'متنوع';
    if (/^RA[: ]/.test(u) || /^RF[: ]/.test(u)) return 'Ra/RF Flix';
    if (/^USA/.test(u)) return 'US Sports';
    if (/^ALWAN SPORT/.test(u)) return 'Alwan Sport';
    if (/^ALWAN/.test(u)) return 'Alwan';
    if (/THAMANYA/.test(u)) return 'Thamanya';
    if (/^OSN|^OSN/i.test(name) && /OSN|OSN/.test(u) && !/^AR/.test(u)) return 'OSN';
    if (/^ALKASS/i.test(u)) return 'Alkass';
    if (/AL FAJER|^ALFAJER/.test(u)) return 'Al Fajer';
    if (/SHAHID/.test(u)) return 'Shahid';
    if (/MBC/.test(u)) return 'MBC';
    if (/STC|\[STC\]/.test(u)) return 'STC';
    if (/WATCHBOX/.test(u)) return 'WatchBox';
    if (/HBO/.test(u)) return 'HBO';
    if (/CANAL\+/.test(u)) return 'CANAL+';
    if (/NETFLIX|^NET[ :|]/.test(u)) return 'NETFLIX';
    if (/AMAZON/.test(u)) return 'AMAZON';
    if (/ITUNES|NEWMAX|BOX OFFICE/.test(u)) return 'Movies';
    if (/OSN|RTV/.test(u)) return 'متنوع';
    if (/^BEIN/i.test(name)) {
        if (/MOVIES|SERIES|DRAMA|STAR |STAR MOVIES|GOURMET|FATAFEAT|HGTV|STARZ|NAT GEO/i.test(name)) return 'beIN Movies';
        if (/ENGLISH|FRENCH/i.test(name)) return 'beIN EN/FR';
        if (/4K/i.test(name)) return 'beIN 4K';
        if (/\bSD\b/i.test(name)) return 'beIN SD';
        if (/\b(FHD|UHD)\b/i.test(name)) return 'beIN FHD/UHD';
        if (/NEWS/i.test(name)) return 'beIN Movies';
        return 'beIN HD';
    }
    if (/\bOSN\b/.test(u)) return 'OSN';
    return 'متنوع';
}

let collisions = 0;
entries.forEach((e, i) => {
    const base = e.name.trim().replace(/[^a-zA-Z0-9\u0600-\u06FF ]+/g, '').replace(/\s+/g, '_').slice(0, 30) || 'ch';
    e.id = `gza_${String(i + 1).padStart(3, '0')}`;
    e.group = classify(e.name);
    if (!e.url) { e.url = 'dummy://no-url'; collisions++; }
});

if (collisions) console.error(`WARNING: ${collisions} entries missing URL`);

const body = entries.map(e => {
    const row = {
        id: e.id,
        name: e.name,
        url: e.url,
        group: e.group
    };
    if (e.ua) row.ua = e.ua;
    if (e.ref) row.ref = e.ref;
    return '    ' + JSON.stringify(row);
}).join(',\n');

const out = `'use strict';\nmodule.exports = {\n  channels: [\n${body}\n  ]\n};\n`;
fs.writeFileSync(OUTPUT, out, 'utf8');

const byGroup = {};
entries.forEach(e => { byGroup[e.group] = (byGroup[e.group] || 0) + 1; });
console.log(`Wrote ${entries.length} channels to ${OUTPUT}`);
Object.entries(byGroup).sort((a, b) => b[1] - a[1]).forEach(([g, c]) => console.log(`  ${g}: ${c}`));