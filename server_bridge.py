import asyncio
import base64
import json
import logging
import os
import socket
import time
import uuid
from aiohttp import web, ClientSession, ClientTimeout

LISTEN_PORT = int(os.environ.get("PORT", 8181))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logging.getLogger('aiohttp.access').setLevel(logging.WARNING)


JS_ENGINE_CODE = """
let responseQueue = [];
let totalBytes = 0;
let packetCount = 0;
let activeSessions = new Set();
let workingEndpoint = null;
let chunkBuffers = {};

function addLog(msg, color) {
    const log = document.getElementById('log');
    if (!log) return;
    const d = document.createElement('div');
    d.className = 'packet';
    if (color) d.style.color = color;
    d.innerHTML = '<span class="time">[' + new Date().toLocaleTimeString() + ']</span> ' + msg;
    log.appendChild(d);
    if (log.childNodes.length > 80) log.removeChild(log.firstChild);
    log.scrollTop = log.scrollHeight;
}

function getCandidateEndpoints() {
    let list = [];
    let p = (window.location.pathname || '').replace(/\\/+$/, '');
    if (p.includes('proxy_u') || p.includes('turbopages.org') || window.location.hostname.includes('yandex') || window.location.hostname.includes('turbopages')) {
        list.push(window.location.origin + p + '/api/batch');
        list.push(window.location.pathname.replace(/\\/+$/, '') + '/api/batch');
    }
    list.push('https://yandex-bridge-dlc6.onrender.com/api/batch');
    list.push('/api/batch');
    return Array.from(new Set(list));
}

function extractTurboPayload(text) {
    if (!text || typeof text !== 'string') return "";
    text = text.trim();

    // 1. Поиск в meta-теге (устойчив к перестановке атрибутов)
    let m = text.match(/name=["']turbo_payload["']\\s+content=["']([^"']+)["']/i) || 
            text.match(/content=["']([^"']+)["']\\s+name=["']turbo_payload["']/i);
    if (m && m[1]) return m[1].trim();

    // 2. Поиск в textarea
    let mArea = text.match(/<textarea[^>]*id=["']turbo_area["'][^>]*>([\\s\\S]*?)<\\/textarea>/i);
    if (mArea && mArea[1]) return mArea[1].trim();

    // 3. Поиск в script
    let mScript = text.match(/<script[^>]*id=["']turbo_data["'][^>]*>([\\s\\S]*?)<\\/script>/i);
    if (mScript && mScript[1]) return mScript[1].trim();

    // 4. Поиск в div
    let mDiv = text.match(/<div[^>]*id=["']turbo_div["'][^>]*>([\\s\\S]*?)<\\/div>/i);
    if (mDiv && mDiv[1]) return mDiv[1].trim();

    // 5. Прямой Base64 (если сервер вернул чистый base64)
    if (!text.includes('<html') && !text.includes('<body') && text.length > 1) {
        return text;
    }
    return "";
}

async function testInternet() {
    const res = document.getElementById('test-res');
    if (res) res.innerHTML = "Проверка IP...";
    addLog("🔍 Проверка внешнего IP сервера...", "#00ff66");
    
    const endpoints = getCandidateEndpoints();
    for (let ep of endpoints) {
        let ipUrl = ep.replace('/api/batch', '/api/ip');
        let sep = ipUrl.includes('?') ? '&' : '?';
        try {
            const r = await fetch(ipUrl + sep + 't=' + Date.now(), { priority: 'high' });
            if (r && r.ok) {
                const text = await r.text();
                let b64 = extractTurboPayload(text);
                if (b64) {
                    const data = JSON.parse(atob(b64));
                    addLog("✓ [IP СЕРВЕРА] " + data.ip + " (" + (data.country || 'Cloud') + ")", "#00ff66");
                    if (res) res.innerHTML = '<span style="color:#00ff66">✓ IP Render: <b>' + data.ip + '</b></span>';
                    return;
                }
            }
        } catch(e) { }
    }
    if (res) res.innerHTML = '<span style="color:#ff3344">✗ Ошибка получения IP</span>';
}

async function sendBatchToServer(b64Data) {
    const endpoints = getCandidateEndpoints();
    let lastError = "";

    // Мелкие куски (до 700 байт) отправляем за 1 GET запрос
    const CHUNK_LEN = 700;
    const cid = Math.random().toString(36).substring(2, 9);
    const totalChunks = Math.ceil(b64Data.length / CHUNK_LEN);

    for (let ep of endpoints) {
        try {
            let finalPayload = "";
            let sep = ep.includes('?') ? '&' : '?';

            // Если пакет маленький (1 кусок)
            if (totalChunks <= 1) {
                let targetUrl = ep + sep + 'p=' + encodeURIComponent(b64Data) + '&t=' + Date.now();
                const resp = await fetch(targetUrl, { priority: 'high' });
                if (resp && resp.ok) {
                    const text = await resp.text();
                    finalPayload = extractTurboPayload(text);
                } else if (resp) {
                    lastError = "HTTP " + resp.status;
                }
            } else {
                // Если пакет большой (дробим на микро-чанки по 700 байт для Яндекс.Переводчика)
                for (let i = 0; i < totalChunks; i++) {
                    let slice = b64Data.substr(i * CHUNK_LEN, CHUNK_LEN);
                    let targetUrl = ep + sep + 'cid=' + cid + '&idx=' + i + '&total=' + totalChunks + '&p=' + encodeURIComponent(slice) + '&t=' + Date.now();
                    const resp = await fetch(targetUrl, { priority: 'high' });
                    if (resp && resp.ok) {
                        const text = await resp.text();
                        if (i === totalChunks - 1) {
                            finalPayload = extractTurboPayload(text);
                        }
                    } else if (resp) {
                        lastError = "HTTP " + resp.status;
                        break;
                    }
                }
            }

            if (finalPayload) {
                return { ok: true, payload: finalPayload };
            }
        } catch (getErr) {
            lastError = getErr.message;
        }
    }
    return { ok: false, error: lastError || "Ошибка связи" };
}

async function yandexMicroChunkEngine() {
    const indLocal = document.getElementById('ind-local');
    const stLocal = document.getElementById('st-local');
    let pollInterval = 10;

    addLog("🚀 Яндекс.Транслятор v28.0 (Micro-Chunk Safe Transport) запущен.", "#00ff66");

    while (true) {
        try {
            const outgoing = responseQueue;
            responseQueue = [];

            const localResp = await fetch('http://localhost:8888/exchange', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(outgoing),
                priority: 'high'
            }).catch(() => null);

            if (!localResp || !localResp.ok) {
                if (indLocal) indLocal.className = 'indicator error';
                if (stLocal) {
                    stLocal.innerText = "ОФЛАЙН";
                    stLocal.style.color = "#ff3344";
                }
                await new Promise(r => setTimeout(r, 1000));
                continue;
            }

            if (indLocal) indLocal.className = 'indicator active';
            if (stLocal) {
                stLocal.innerText = "ПОДКЛЮЧЕН (YANDEX-SAFE)";
                stLocal.style.color = "#00ff66";
            }

            const tasks = await localResp.json();

            if (tasks && tasks.length > 0 && tasks[0].sid !== "IDLE") {
                const b64Data = btoa(JSON.stringify(tasks));
                const res = await sendBatchToServer(b64Data);

                if (res.ok && res.payload) {
                    try {
                        const results = JSON.parse(atob(res.payload));
                        let recvBytes = 0;
                        for (let item of results) {
                            if (item.data && item.data !== "EMPTY") {
                                responseQueue.push(item);
                                if (item.data !== "EOF") {
                                    recvBytes += item.data.length;
                                }
                            }
                            if (item.data === "EOF" || item.type === "close") {
                                activeSessions.delete(item.sid);
                            } else if (item.sid && item.sid !== "IDLE") {
                                activeSessions.add(item.sid);
                            }
                            packetCount++;
                        }
                        totalBytes += b64Data.length;
                        if (recvBytes > 0) {
                            addLog('⚡ Получено: ' + (recvBytes > 1024 ? (recvBytes/1024).toFixed(1) + ' KB' : recvBytes + 'b'), '#00ff66');
                            pollInterval = 0;
                        } else {
                            pollInterval = 10;
                        }
                        
                        const bElem = document.getElementById('bytes');
                        if (bElem) bElem.innerText = (totalBytes / 1024).toFixed(1) + " KB";
                        const pkElem = document.getElementById('pk-count');
                        if (pkElem) pkElem.innerText = packetCount;
                        const sessElem = document.getElementById('sess-count');
                        if (sessElem) sessElem.innerText = activeSessions.size;
                    } catch(jsonErr) {
                        addLog('❌ Ошибка JSON: ' + jsonErr.message, '#ff3344');
                    }
                } else {
                    pollInterval = 25;
                }
            } else {
                pollInterval = 60;
            }
        } catch (e) { }
        await new Promise(r => setTimeout(r, pollInterval));
    }
}

yandexMicroChunkEngine();
"""

JS_BASE64 = base64.b64encode(JS_ENGINE_CODE.strip().encode('utf-8')).decode('utf-8')


class YandexRenderBridgeServer:
    def __init__(self):
        self.sessions = {}
        self.pending_buffers = {}
        self.chunk_assembler = {}  # cid -> {'chunks': {}, 'total': N, 'time': t}
        self.buffer_lock = asyncio.Lock()
        self.total_bytes_rx = 0
        self.total_bytes_tx = 0
        self.start_time = time.time()

    def format_turbo_response(self, b64_data):
        return (
            f'<!DOCTYPE html>'
            f'<html lang="en" translate="no" class="notranslate">'
            f'<head>'
            f'<meta charset="UTF-8">'
            f'<meta name="google" content="notranslate">'
            f'<meta name="yandex" content="notranslate">'
            f'<meta name="turbo_payload" content="{b64_data}">'
            f'</head>'
            f'<body translate="no" class="notranslate">'
            f'<textarea id="turbo_area" class="notranslate" translate="no" style="display:none">{b64_data}</textarea>'
            f'<div id="turbo_div" class="notranslate" translate="no" style="display:none">{b64_data}</div>'
            f'<script id="turbo_data" translate="no">{b64_data}</script>'
            f'</body></html>'
        )

    async def handle_ping(self, request):
        cors = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Private-Network': 'true',
            'Cache-Control': 'no-store, no-cache, must-revalidate'
        }
        return web.json_response({
            "status": "online",
            "uptime_sec": int(time.time() - self.start_time),
            "active_sessions": len(self.sessions),
            "timestamp": time.time()
        }, headers=cors)

    async def handle_get_ip(self, request):
        cors = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
            'Access-Control-Allow-Headers': '*',
            'Cache-Control': 'no-store, no-cache, must-revalidate'
        }
        if request.method == "OPTIONS":
            return web.Response(headers=cors)

        try:
            timeout = ClientTimeout(total=5.0)
            async with ClientSession(timeout=timeout) as s:
                async with s.get('https://api.ipify.org?format=json') as r:
                    data = await r.json()
                    data["country"] = "Render Cloud"
                    b64 = base64.b64encode(json.dumps(data).encode()).decode()
                    res_html = self.format_turbo_response(b64)
                    return web.Response(text=res_html, content_type='text/html', headers=cors)
        except Exception as e:
            err_data = {"ip": f"Ошибка: {e}"}
            b64 = base64.b64encode(json.dumps(err_data).encode()).decode()
            res_html = self.format_turbo_response(b64)
            return web.Response(text=res_html, content_type='text/html', headers=cors)

    async def get_dashboard(self, request):
        headers = {
            'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
            'Pragma': 'no-cache',
            'Expires': '0'
        }
        html = f"""<!DOCTYPE html>
<html lang="en" translate="no" class="notranslate">
<head>
    <meta charset="UTF-8">
    <meta name="google" content="notranslate">
    <meta name="yandex" content="notranslate">
    <meta name="robots" content="noindex, nofollow, notranslate">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>⚡ Yandex Relay Bridge v28.0 (Micro-Chunk Safe)</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: 'Segoe UI', Tahoma, monospace, sans-serif; background: #06080c; color: #00ff66; padding: 15px; }}
        .header {{ display: flex; align-items: center; justify-content: space-between; padding-bottom: 12px; border-bottom: 1px solid #1a2230; margin-bottom: 15px; }}
        h1 {{ font-size: 18px; color: #fff; font-weight: 600; }}
        h1 span {{ color: #00ff66; font-size: 13px; font-weight: normal; margin-left: 8px; }}
        .grid {{ display: grid; grid-template-columns: 1fr 320px; gap: 15px; }}
        @media(max-width: 800px) {{ .grid {{ grid-template-columns: 1fr; }} }}
        #log {{ background: #020305; border: 1px solid #161e2b; height: 500px; overflow-y: auto; padding: 12px; font-family: 'Consolas', 'Courier New', monospace; font-size: 12px; border-radius: 6px; }}
        .card {{ background: #0e131d; border: 1px solid #1a2332; padding: 16px; border-radius: 6px; }}
        .packet {{ border-bottom: 1px solid #141b27; padding: 4px 0; color: #8fa3bf; word-break: break-all; }}
        .packet b {{ color: #00ff66; }}
        .packet .time {{ color: #506177; font-size: 11px; margin-right: 5px; }}
        .indicator {{ width: 10px; height: 10px; border-radius: 50%; display: inline-block; background: #555; margin-right: 8px; }}
        .active {{ background: #00ff66; box-shadow: 0 0 8px #00ff66; }}
        .error {{ background: #ff3344; box-shadow: 0 0 8px #ff3344; }}
        .stat {{ margin-bottom: 12px; font-size: 13px; color: #ccd6e0; display: flex; justify-content: space-between; }}
        .stat b {{ color: #00ff66; }}
        button {{ background: #00ff66; color: #000; border: none; padding: 10px 15px; border-radius: 4px; cursor: pointer; font-weight: 700; width: 100%; margin-top: 8px; transition: 0.2s; }}
        button:hover {{ background: #00cc52; }}
    </style>
</head>
<body translate="no" class="notranslate">
    <div class="header">
        <h1>⚡ RENDER CLOUD TUNNEL <span>[Micro-Chunk v28.0]</span></h1>
        <div style="font-size: 12px; color: #00ff66;">● RENDER: ONLINE</div>
    </div>

    <a id="proxy_path" href="/api/batch" style="display:none"></a>

    <div class="grid">
        <div id="log"></div>
        <div class="card">
            <div class="stat">
                <span><span id="ind-local" class="indicator"></span>Локальный клиент:</span>
                <b id="st-local" style="color:#ff3344">ОФЛАЙН</b>
            </div>
            <div class="stat"><span>Трафик туннеля:</span> <b id="bytes">0.0 KB</b></div>
            <div class="stat"><span>Обработано пакетов:</span> <b id="pk-count">0</b></div>
            <div class="stat"><span>Активные сессии:</span> <b id="sess-count">0</b></div>
            <hr style="border:0; border-top:1px solid #1a2332; margin: 15px 0;">
            <button onclick="testInternet()">🔍 ПРОВЕРИТЬ ВНЕШНИЙ IP СЕРВЕРА</button>
            <div id="test-res" style="margin-top:10px; font-size:12px; color:#8fa3bf; text-align:center;"></div>
        </div>
    </div>

    <script translate="no" class="notranslate">
        eval(decodeURIComponent(escape(atob("{JS_BASE64}"))));
    </script>
</body>
</html>"""
        return web.Response(text=html, content_type='text/html', headers=headers)

    async def handle_batch(self, request):
        cors = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Private-Network': 'true',
            'Cache-Control': 'no-store, no-cache, must-revalidate'
        }
        if request.method == "OPTIONS":
            return web.Response(headers=cors)

        try:
            cid = request.query.get("cid")
            idx = request.query.get("idx")
            total = request.query.get("total")
            b64_payload = request.query.get("p", "")

            # Сборка многосегментного микро-чанка
            if cid and idx is not None and total is not None:
                idx = int(idx)
                total = int(total)
                if cid not in self.chunk_assembler:
                    self.chunk_assembler[cid] = {'chunks': {}, 'total': total, 'time': time.time()}
                self.chunk_assembler[cid]['chunks'][idx] = b64_payload

                if len(self.chunk_assembler[cid]['chunks']) == total:
                    full_p = "".join(self.chunk_assembler[cid]['chunks'][i] for i in range(total))
                    del self.chunk_assembler[cid]
                    b64_payload = full_p
                else:
                    # Промежуточный чанк принят
                    res_html = self.format_turbo_response(base64.b64encode(b"[]").decode())
                    return web.Response(text=res_html, content_type='text/html', headers=cors)

            if not b64_payload:
                res_html = self.format_turbo_response(base64.b64encode(b"[]").decode())
                return web.Response(text=res_html, content_type='text/html', headers=cors)

            tasks = json.loads(base64.b64decode(b64_payload).decode('utf-8', 'ignore'))
            results = []
            for t in tasks:
                res = await self.process_packet(t)
                results.append(res)

            resp_b64 = base64.b64encode(json.dumps(results).encode()).decode()
            res_html = self.format_turbo_response(resp_b64)
            return web.Response(text=res_html, content_type='text/html', headers=cors)
        except Exception as e:
            logging.error(f"Error in handle_batch: {e}")
            res_html = self.format_turbo_response(base64.b64encode(b"[]").decode())
            return web.Response(text=res_html, content_type='text/html', headers=cors)

    async def process_packet(self, packet):
        sid = packet.get("sid")
        p_type = packet.get("t")
        payload = packet.get("p")
        resp_data = "EMPTY"

        if not sid or sid == "IDLE":
            return {"sid": "IDLE", "data": "EMPTY", "type": "idle"}

        if p_type == "connect":
            try:
                info = json.loads(base64.b64decode(payload).decode('utf-8', 'ignore'))
                addr = str(info['addr']).strip(" \t\n\r\x00/\"'")
                port = int(info['port'])
                await self.open_connection(sid, addr, port)
                return {"sid": sid, "data": "EMPTY", "type": "connect"}
            except Exception as e:
                logging.error(f"[{sid}] Connect error: {e}")
                return {"sid": sid, "data": "EOF", "type": "connect"}

        elif p_type == "data" and payload:
            raw_data = base64.b64decode(payload)
            if sid in self.sessions and self.sessions[sid]['active']:
                try:
                    self.sessions[sid]['w'].write(raw_data)
                    await self.sessions[sid]['w'].drain()
                    self.total_bytes_rx += len(raw_data)
                except Exception as e:
                    logging.debug(f"[{sid}] Write error: {e}")
                    self.sessions[sid]['active'] = False
            else:
                if sid not in self.pending_buffers:
                    self.pending_buffers[sid] = bytearray()
                self.pending_buffers[sid].extend(raw_data)

        elif p_type == "close":
            if sid in self.sessions:
                try:
                    self.sessions[sid]['w'].close()
                except:
                    pass
                del self.sessions[sid]
            if sid in self.pending_buffers:
                del self.pending_buffers[sid]
            return {"sid": sid, "data": "EOF", "type": "close"}

        if sid in self.sessions:
            async with self.buffer_lock:
                self.sessions[sid]['last_poll'] = time.time()
                if len(self.sessions[sid]['buffer']) > 0:
                    resp_data = base64.b64encode(self.sessions[sid]['buffer']).decode()
                    self.total_bytes_tx += len(self.sessions[sid]['buffer'])
                    self.sessions[sid]['buffer'] = bytearray()
                elif not self.sessions[sid]['active']:
                    resp_data = "EOF"
                    del self.sessions[sid]

        return {"sid": sid, "data": resp_data, "type": p_type}

    async def open_connection(self, sid, addr, port):
        try:
            r, w = await asyncio.wait_for(
                asyncio.open_connection(addr, port, family=socket.AF_INET),
                timeout=8.0
            )
            sock = w.get_extra_info('socket')
            if sock:
                try:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2097152)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2097152)
                except:
                    pass

            self.sessions[sid] = {
                'w': w,
                'r': r,
                'buffer': bytearray(),
                'active': True,
                'last_poll': time.time()
            }
            if sid in self.pending_buffers and self.pending_buffers[sid]:
                w.write(self.pending_buffers[sid])
                await w.drain()
                self.total_bytes_rx += len(self.pending_buffers[sid])
                del self.pending_buffers[sid]

            asyncio.create_task(self.read_remote(sid, r))
        except Exception as e:
            logging.error(f"[-] [{sid}] Connect failed ({addr}:{port}): {e}")

    async def read_remote(self, sid, reader):
        try:
            while sid in self.sessions and self.sessions[sid]['active']:
                try:
                    data = await asyncio.wait_for(reader.read(1048576), timeout=0.25)
                    if not data:
                        break
                    async with self.buffer_lock:
                        if sid in self.sessions:
                            self.sessions[sid]['buffer'].extend(data)
                except asyncio.TimeoutError:
                    continue
                except (ConnectionResetError, ConnectionAbortedError):
                    break
                except Exception:
                    break
        finally:
            if sid in self.sessions:
                self.sessions[sid]['active'] = False

    async def session_cleaner(self):
        while True:
            await asyncio.sleep(15)
            now = time.time()
            async with self.buffer_lock:
                to_del = [sid for sid, s in self.sessions.items() if now - s['last_poll'] > 30 and len(s['buffer']) == 0]
                for sid in to_del:
                    if sid in self.sessions:
                        try:
                            self.sessions[sid]['w'].close()
                        except:
                            pass
                        del self.sessions[sid]
                # Очистка устаревших чанков
                expired_cids = [c for c, obj in self.chunk_assembler.items() if now - obj['time'] > 20]
                for c in expired_cids:
                    del self.chunk_assembler[c]

    async def run(self):
        app = web.Application(client_max_size=50*1024*1024)
        app.router.add_get('/healthz', self.handle_ping)
        app.router.add_get('/api/ping', self.handle_ping)
        app.router.add_route('*', '/api/ip', self.handle_get_ip)
        app.router.add_route('*', '/api/ip/{tail:.*}', self.handle_get_ip)
        app.router.add_route('*', '/api/batch', self.handle_batch)
        app.router.add_route('*', '/api/batch/{tail:.*}', self.handle_batch)
        app.router.add_get('/', self.get_dashboard)
        app.router.add_get('/{tail:.*}', self.get_dashboard)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', LISTEN_PORT)
        await site.start()

        logging.info(f"==================================================")
        logging.info(f"🚀 RENDER BRIDGE SERVER v28.0 READY ON PORT {LISTEN_PORT}")
        logging.info(f"==================================================")
        await self.session_cleaner()


if __name__ == "__main__":
    try:
        asyncio.run(YandexRenderBridgeServer().run())
    except KeyboardInterrupt:
        pass
