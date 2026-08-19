import asyncio
import base64
import json
import logging
import os
import socket
import time
from aiohttp import web

# Render.com передает порт через переменную окружения PORT
LISTEN_PORT = int(os.environ.get("PORT", 8181))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logging.getLogger('aiohttp.access').setLevel(logging.WARNING)


class YandexRenderBridgeServer:
    def __init__(self):
        # sid -> {'w': writer, 'r': reader, 'buffer': bytearray(), 'active': bool, 'last_poll': float}
        self.sessions = {}
        self.buffer_lock = asyncio.Lock()
        self.total_bytes_rx = 0
        self.total_bytes_tx = 0
        self.start_time = time.time()

    async def handle_ping(self, request):
        cors = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Private-Network': 'true'
        }
        return web.json_response({
            "status": "online",
            "uptime_sec": int(time.time() - self.start_time),
            "active_sessions": len(self.sessions),
            "timestamp": time.time()
        }, headers=cors)

    async def get_dashboard(self, request):
        """Дашборд и движок ретрансляции, адаптированный под проксирование Яндекс.Переводчика"""
        html = """<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>⚡ Yandex Relay Bridge</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Segoe UI', Tahoma, monospace, sans-serif; background: #06080c; color: #00ff66; padding: 15px; }
        .header { display: flex; align-items: center; justify-content: space-between; padding-bottom: 12px; border-bottom: 1px solid #1a2230; margin-bottom: 15px; }
        h1 { font-size: 18px; color: #fff; font-weight: 600; }
        h1 span { color: #00ff66; font-size: 13px; font-weight: normal; margin-left: 8px; }
        .grid { display: grid; grid-template-columns: 1fr 320px; gap: 15px; }
        @media(max-width: 800px) { .grid { grid-template-columns: 1fr; } }
        #log { background: #020305; border: 1px solid #161e2b; height: 500px; overflow-y: auto; padding: 12px; font-family: 'Consolas', 'Courier New', monospace; font-size: 12px; border-radius: 6px; }
        .card { background: #0e131d; border: 1px solid #1a2332; padding: 16px; border-radius: 6px; }
        .packet { border-bottom: 1px solid #141b27; padding: 4px 0; color: #8fa3bf; word-break: break-all; }
        .packet b { color: #00ff66; }
        .packet .time { color: #506177; font-size: 11px; margin-right: 5px; }
        .indicator { width: 10px; height: 10px; border-radius: 50%; display: inline-block; background: #555; margin-right: 8px; }
        .active { background: #00ff66; box-shadow: 0 0 8px #00ff66; }
        .error { background: #ff3344; box-shadow: 0 0 8px #ff3344; }
        .stat { margin-bottom: 12px; font-size: 13px; color: #ccd6e0; display: flex; justify-content: space-between; }
        .stat b { color: #00ff66; }
        button { background: #00ff66; color: #000; border: none; padding: 10px 15px; border-radius: 4px; cursor: pointer; font-weight: 700; width: 100%; margin-top: 8px; transition: 0.2s; }
        button:hover { background: #00cc52; }
        .unlock-btn { display: none; background: #ff3344; color: #fff; padding: 12px; text-align: center; text-decoration: none; font-weight: bold; border-radius: 4px; margin-bottom: 12px; font-size: 12px; animation: pulse 2s infinite; }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
    </style>
</head>
<body>
    <div class="header">
        <h1>⚡ RENDER CLOUD TUNNEL <span>[Turbo Bridge v6.0]</span></h1>
        <div style="font-size: 12px; color: #00ff66;">● RENDER: ONLINE</div>
    </div>

    <!-- Скрытая ссылка для автоматического вычисления URL Яндекс-прокси -->
    <a id="proxy_path" href="/api/batch" style="display:none"></a>

    <a id="unlock_btn" href="http://127.0.0.1:8888/exchange" target="_blank" class="unlock-btn">
        ⚠️ НАЖМИТЕ ЗДЕСЬ ДЛЯ РАЗРЕШЕНИЯ ДОСТУПА К ЛОКАЛЬНОМУ КЛИЕНТУ
    </a>

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
            <button onclick="testInternet()">🔍 ПРОВЕРИТЬ ИНТЕРНЕТ СЕРВЕРА</button>
            <div id="test-res" style="margin-top:10px; font-size:12px; color:#8fa3bf; text-align:center;"></div>
        </div>
    </div>

    <script>
        let responseQueue = [];
        let totalBytes = 0;
        let packetCount = 0;
        let activeSessions = new Set();
        const log = document.getElementById('log');
        const indLocal = document.getElementById('ind-local');
        const stLocal = document.getElementById('st-local');
        const unlockBtn = document.getElementById('unlock_btn');

        function addLog(msg) {
            const d = document.createElement('div');
            d.className = 'packet';
            d.innerHTML = `<span class="time">[${new Date().toLocaleTimeString()}]</span> ${msg}`;
            log.appendChild(d);
            if (log.childNodes.length > 50) log.removeChild(log.firstChild);
            log.scrollTop = log.scrollHeight;
        }

        async function testInternet() {
            const res = document.getElementById('test-res');
            res.innerText = "Проверка...";
            try {
                const r = await fetch('https://api.ipify.org?format=json');
                const data = await r.json();
                res.innerHTML = `<span style="color:#00ff66">✓ IP Сервера: <b>${data.ip}</b></span>`;
            } catch(e) {
                res.innerHTML = `<span style="color:#ff3344">✗ Ошибка внешнего IP</span>`;
            }
        }

        // Обработка данных, полученных от локального клиента
        window.handleClientTasks = async function(tasks) {
            indLocal.className = 'indicator active';
            stLocal.innerText = "ПОДКЛЮЧЕН";
            stLocal.style.color = "#00ff66";
            unlockBtn.style.display = 'none';

            if (tasks && tasks.length > 0 && tasks[0].sid !== "IDLE") {
                const proxyAnchor = document.getElementById('proxy_path');
                const proxyUrl = proxyAnchor ? proxyAnchor.href : '/api/batch';
                
                try {
                    const encodedPayload = encodeURIComponent(btoa(JSON.stringify(tasks)));
                    const serverResp = await fetch(proxyUrl + (proxyUrl.includes('?') ? '&' : '?') + 't=' + Date.now(), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                        body: 'p=' + encodedPayload
                    });

                    if (serverResp.ok) {
                        const htmlText = await serverResp.text();
                        if (htmlText.includes('id="turbo_data">')) {
                            const rawB64 = htmlText.split('id="turbo_data">')[1].split('</script>')[0].trim();
                            const results = JSON.parse(atob(rawB64));
                            
                            responseQueue = [];
                            for (let res of results) {
                                if (res.data && res.data !== "EMPTY") {
                                    responseQueue.push(res);
                                }
                                if (res.sid && res.sid !== "IDLE") {
                                    activeSessions.add(res.sid);
                                }
                                packetCount++;
                                addLog(`<b>${res.sid}</b> | ${res.type || 'data'} ${res.data && res.data !== 'EMPTY' ? '⚡ ' + res.data.length + 'b' : ''}`);
                            }
                            totalBytes += encodedPayload.length;
                            document.getElementById('bytes').innerText = (totalBytes / 1024).toFixed(1) + " KB";
                            document.getElementById('pk-count').innerText = packetCount;
                            document.getElementById('sess-count').innerText = activeSessions.size;
                        }
                    }
                } catch(e) {
                    console.error("Server relay error:", e);
                }
            }
        };

        async function relayLoop() {
            // 1. Сначала пробуем Fetch с поддержкой Private Network Access
            try {
                const sendBody = JSON.stringify(responseQueue);
                responseQueue = [];
                const localResp = await fetch('http://127.0.0.1:8888/exchange', {
                    method: 'POST',
                    mode: 'cors',
                    headers: { 'Content-Type': 'application/json' },
                    body: sendBody
                }).catch(() => null);

                if (localResp && localResp.ok) {
                    const tasks = await localResp.json();
                    await window.handleClientTasks(tasks);
                    setTimeout(relayLoop, 40);
                    return;
                }
            } catch(e) { }

            // 2. Если Fetch заблокирован браузером (PNA), используем JSONP как резерв
            try {
                const script = document.createElement('script');
                const q = encodeURIComponent(JSON.stringify(responseQueue));
                responseQueue = [];
                script.src = `http://127.0.0.1:8888/exchange?callback=handleClientTasks&data=${q}&t=${Date.now()}`;
                
                script.onerror = () => {
                    indLocal.className = 'indicator error';
                    stLocal.innerText = "ОФЛАЙН";
                    stLocal.style.color = "#ff3344";
                    unlockBtn.style.display = 'block';
                };
                
                document.body.appendChild(script);
                setTimeout(() => { script.remove(); }, 1000);
            } catch(e) { }

            setTimeout(relayLoop, 150);
        }

        addLog("Транспортный движок моста запущен.");
        relayLoop();
    </script>
</body>
</html>"""
        return web.Response(text=html, content_type='text/html', headers={'Cache-Control': 'no-cache'})

    async def handle_batch(self, request):
        cors = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Private-Network': 'true'
        }
        if request.method == "OPTIONS":
            return web.Response(headers=cors)

        try:
            # Читаем form-data 'p' или querystring
            b64_payload = ""
            if request.method == "POST":
                post_data = await request.post()
                b64_payload = post_data.get("p")
                if not b64_payload:
                    try:
                        raw_body = await request.text()
                        if raw_body.startswith("p="):
                            from urllib.parse import unquote
                            b64_payload = unquote(raw_body[2:])
                    except:
                        pass
            if not b64_payload:
                b64_payload = request.query.get("p")

            if not b64_payload:
                res_html = '<html><body><script id="turbo_data">' + base64.b64encode(b"[]").decode() + '</script></body></html>'
                return web.Response(text=res_html, content_type='text/html', headers=cors)

            tasks = json.loads(base64.b64decode(b64_payload).decode('utf-8', 'ignore'))
            results = []
            for t in tasks:
                res = await self.process_packet(t)
                results.append(res)

            resp_b64 = base64.b64encode(json.dumps(results).encode()).decode()
            # Оборачиваем в <script id="turbo_data"> чтобы Яндекс.Переводчик не искажал Base64
            res_html = f'<html><body><script id="turbo_data">{resp_b64}</script></body></html>'
            return web.Response(text=res_html, content_type='text/html', headers=cors)
        except Exception as e:
            logging.error(f"Error in handle_batch: {e}")
            res_html = '<html><body><script id="turbo_data">' + base64.b64encode(b"[]").decode() + '</script></body></html>'
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
                return {"sid": sid, "data": "OK", "type": "connect"}
            except Exception as e:
                logging.error(f"[{sid}] Connect parse error: {e}")
                return {"sid": sid, "data": "ERROR", "type": "connect"}

        elif p_type == "data" and payload:
            if sid in self.sessions and self.sessions[sid]['active']:
                try:
                    raw_data = base64.b64decode(payload)
                    self.sessions[sid]['w'].write(raw_data)
                    await self.sessions[sid]['w'].drain()
                    self.total_bytes_rx += len(raw_data)
                except Exception as e:
                    logging.debug(f"[{sid}] Write error: {e}")
                    self.sessions[sid]['active'] = False

        elif p_type == "close":
            if sid in self.sessions:
                try:
                    self.sessions[sid]['w'].close()
                except:
                    pass
                del self.sessions[sid]
                return {"sid": sid, "data": "EOF", "type": "close"}

        # Проверяем буфер ответов для этой сессии
        if sid in self.sessions:
            async with self.buffer_lock:
                self.sessions[sid]['last_poll'] = time.time()
                if self.sessions[sid]['buffer']:
                    resp_data = base64.b64encode(self.sessions[sid]['buffer']).decode()
                    self.total_bytes_tx += len(self.sessions[sid]['buffer'])
                    self.sessions[sid]['buffer'] = bytearray()
                elif not self.sessions[sid]['active']:
                    resp_data = "EOF"
                    del self.sessions[sid]

        return {"sid": sid, "data": resp_data, "type": p_type}

    async def open_connection(self, sid, addr, port):
        """Открытие исходящего TCP соединения во внешний интернет"""
        try:
            logging.info(f"[*] [{sid}] Connecting to {addr}:{port}...")
            r, w = await asyncio.wait_for(
                asyncio.open_connection(addr, port, family=socket.AF_INET),
                timeout=12.0
            )
            self.sessions[sid] = {
                'w': w,
                'r': r,
                'buffer': bytearray(),
                'active': True,
                'last_poll': time.time()
            }
            asyncio.create_task(self.read_remote(sid, r))
            logging.info(f"[+] [{sid}] Connected to {addr}:{port}")
        except Exception as e:
            logging.error(f"[-] [{sid}] Connection failed ({addr}:{port}): {e}")

    async def read_remote(self, sid, reader):
        """Чтение данных от удаленного хоста в буфер"""
        try:
            while sid in self.sessions and self.sessions[sid]['active']:
                try:
                    data = await asyncio.wait_for(reader.read(32768), timeout=1.0)
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
        """Очистка старых сессий"""
        while True:
            await asyncio.sleep(20)
            now = time.time()
            async with self.buffer_lock:
                to_del = [sid for sid, s in self.sessions.items() if now - s['last_poll'] > 60]
                for sid in to_del:
                    if sid in self.sessions:
                        try:
                            self.sessions[sid]['w'].close()
                        except:
                            pass
                        del self.sessions[sid]
                        logging.info(f"[X] Cleaned session {sid}")

    async def run(self):
        app = web.Application()
        app.router.add_get('/healthz', self.handle_ping)
        app.router.add_get('/api/ping', self.handle_ping)
        app.router.add_route('*', '/api/batch', self.handle_batch)
        app.router.add_get('/', self.get_dashboard)
        app.router.add_get('/{tail:.*}', self.get_dashboard)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', LISTEN_PORT)
        await site.start()

        logging.info(f"==================================================")
        logging.info(f"🚀 RENDER BRIDGE SERVER v6.0 READY ON PORT {LISTEN_PORT}")
        logging.info(f"==================================================")
        await self.session_cleaner()


if __name__ == "__main__":
    try:
        asyncio.run(YandexRenderBridgeServer().run())
    except KeyboardInterrupt:
        pass
