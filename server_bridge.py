import asyncio
import base64
import json
import logging
import os
import time
from aiohttp import web

# --- КОНФИГУРАЦИЯ СЕРВЕРА ---
# Render.com автоматически передает порт через переменную окружения PORT
LISTEN_PORT = int(os.environ.get("PORT", 8181))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logging.getLogger('aiohttp.access').setLevel(logging.WARNING)


class YandexRenderBridgeServer:
    def __init__(self):
        # sid -> {'writer': writer, 'reader': reader, 'buffer': bytearray(), 'active': bool, 'last_poll': float}
        self.sessions = {}
        self.buffer_lock = asyncio.Lock()
        self.total_bytes_rx = 0
        self.total_bytes_tx = 0
        self.start_time = time.time()

    async def handle_ping(self, request):
        """Эндпоинт для проверки статуса и предотвращения сна Render.com"""
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
            'Access-Control-Allow-Headers': '*'
        }
        return web.json_response({
            "status": "online",
            "uptime_sec": int(time.time() - self.start_time),
            "active_sessions": len(self.sessions),
            "timestamp": time.time()
        }, headers=headers)

    async def get_dashboard(self, request):
        """Главная веб-страница, открываемая через Яндекс.Переводчик для работы AJAX-моста"""
        # Проверка на рукопожатие или ботов
        if "data_" in request.path or "d=" in request.query_string:
            return web.Response(text="---BRIDGE_START---ALIVE:Render Bridge Active---BRIDGE_END---")

        html = """<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>⚡ Yandex-Render Relay Bridge</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; background: #0a0c10; color: #00ff66; padding: 15px; }
        .header { display: flex; align-items: center; justify-content: space-between; padding-bottom: 12px; border-bottom: 1px solid #1f242c; margin-bottom: 15px; }
        h1 { font-size: 20px; color: #fff; font-weight: 600; }
        h1 span { color: #00ff66; font-size: 14px; margin-left: 8px; font-weight: normal; }
        .grid { display: grid; grid-template-columns: 1fr 340px; gap: 15px; }
        @media(max-width: 850px) { .grid { grid-template-columns: 1fr; } }
        #log { background: #040507; border: 1px solid #1a202c; height: 500px; overflow-y: auto; padding: 12px; font-family: 'Consolas', 'Courier New', monospace; font-size: 12px; border-radius: 6px; box-shadow: inset 0 0 10px rgba(0,0,0,0.8); }
        .card { background: #12161f; border: 1px solid #1f2633; padding: 16px; border-radius: 6px; }
        .packet { border-bottom: 1px solid #161b24; padding: 4px 0; color: #8fa3bf; word-break: break-all; }
        .packet b { color: #00ff66; }
        .packet .time { color: #506177; font-size: 11px; margin-right: 5px; }
        .indicator { width: 10px; height: 10px; border-radius: 50%; display: inline-block; background: #555; margin-right: 8px; }
        .active { background: #00ff66; box-shadow: 0 0 8px #00ff66; }
        .error { background: #ff3344; box-shadow: 0 0 8px #ff3344; }
        .stat { margin-bottom: 12px; font-size: 13px; color: #ccd6e0; display: flex; justify-content: space-between; }
        .stat b { color: #00ff66; }
        button { background: #00ff66; color: #000; border: none; padding: 10px 15px; border-radius: 4px; cursor: pointer; font-weight: 700; width: 100%; margin-top: 8px; transition: 0.2s; }
        button:hover { background: #00cc52; }
        .tag { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: 600; }
        .tag-ok { background: rgba(0,255,102,0.15); color: #00ff66; }
        .tag-off { background: rgba(255,51,68,0.15); color: #ff3344; }
    </style>
</head>
<body>
    <div class="header">
        <h1>⚡ RENDER CLOUD TUNNEL <span>v5.2 [Batch Mode]</span></h1>
        <div id="bridge-status"><span class="tag tag-ok">RENDER: ONLINE</span></div>
    </div>
    <div class="grid">
        <div id="log"></div>
        <div class="card">
            <div class="stat">
                <span><span id="ind-local" class="indicator"></span>Локальный клиент:</span>
                <b id="st-local" style="color:#ff3344">ОФЛАЙН</b>
            </div>
            <div class="stat"><span>Трафик туннеля:</span> <b id="bytes">0.0 KB</b></div>
            <div class="stat"><span>Активные сессии:</span> <b id="sess-count">0</b></div>
            <div class="stat"><span>Пакеты в очереди:</span> <b id="q-count">0</b></div>
            <hr style="border:0; border-top:1px solid #1f2633; margin: 15px 0;">
            <button onclick="testInternet()">🔍 ПРОВЕРИТЬ ИНТЕРНЕТ СЕРВЕРА</button>
            <div id="test-res" style="margin-top:10px; font-size:12px; color:#8fa3bf; text-align:center;"></div>
        </div>
    </div>

    <script>
        let responseQueue = [];
        let totalBytes = 0;
        let activeSessionsMap = new Set();

        async function testInternet() {
            const res = document.getElementById('test-res');
            res.innerText = "Проверка...";
            try {
                const r = await fetch('https://api.ipify.org?format=json');
                const data = await r.json();
                res.innerHTML = `<span style="color:#00ff66">✓ IP Сервера: <b>${data.ip}</b></span>`;
            } catch(e) {
                res.innerHTML = `<span style="color:#ff3344">✗ Ошибка доступа к внешнему IP</span>`;
            }
        }

        async function relayLoop() {
            const log = document.getElementById('log');
            const indLocal = document.getElementById('ind-local');
            const stLocal = document.getElementById('st-local');
            
            while(true) {
                try {
                    // 1. Опрос локального клиента (на localhost:8888)
                    const localResp = await fetch('http://localhost:8888/exchange', {
                        method: 'POST',
                        mode: 'cors',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify(responseQueue)
                    }).catch(() => null);

                    if (!localResp || !localResp.ok) {
                        indLocal.className = 'indicator error';
                        stLocal.innerText = "ОФЛАЙН";
                        stLocal.style.color = "#ff3344";
                        await new Promise(r => setTimeout(r, 1000));
                        continue;
                    }

                    indLocal.className = 'indicator active';
                    stLocal.innerText = "ПОДКЛЮЧЕН";
                    stLocal.style.color = "#00ff66";
                    responseQueue = []; // очищаем отправленные ответы

                    const tasks = await localResp.json();
                    document.getElementById('q-count').innerText = tasks.length;

                    if (tasks.length > 0 && tasks[0].sid !== "IDLE") {
                        // 2. Отправка пакетов на Render-сервер
                        const serverResp = await fetch('/api/batch', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify(tasks)
                        });
                        
                        if (serverResp.ok) {
                            const results = await serverResp.json();
                            for (let res of results) {
                                if (res.data && res.data !== "EMPTY") {
                                    responseQueue.push(res);
                                }
                                if (res.sid && res.sid !== "IDLE") {
                                    activeSessionsMap.add(res.sid);
                                }
                                const d = document.createElement('div');
                                d.className = 'packet';
                                d.innerHTML = `<span class="time">[${new Date().toLocaleTimeString()}]</span> <b>${res.sid}</b> | <span>${res.type || 'data'}</span> ${res.data && res.data !== 'EMPTY' ? '⚡ ' + res.data.length + 'b' : ''}`;
                                log.appendChild(d);
                                if (log.childNodes.length > 35) log.removeChild(log.firstChild);
                            }
                            totalBytes += JSON.stringify(tasks).length;
                            document.getElementById('bytes').innerText = (totalBytes / 1024).toFixed(1) + " KB";
                            document.getElementById('sess-count').innerText = activeSessionsMap.size;
                            log.scrollTop = log.scrollHeight;
                        }
                    }
                } catch(e) {
                    console.error("Relay error:", e);
                }
                await new Promise(r => setTimeout(r, 50));
            }
        }

        // Запуск цикла ретрансляции
        relayLoop();
    </script>
</body>
</html>"""
        return web.Response(text=html, content_type='text/html')

    async def handle_batch(self, request):
        """Пакетная обработка задач туннеля от браузера"""
        try:
            tasks = await request.json()
            results = []
            for t in tasks:
                res = await self.process_packet(t)
                results.append(res)
            return web.json_response(results)
        except Exception as e:
            logging.error(f"Error in handle_batch: {e}")
            return web.json_response([], status=400)

    async def process_packet(self, packet):
        sid = packet.get("sid")
        p_type = packet.get("t")
        payload = packet.get("p")
        resp_data = "EMPTY"

        if p_type == "connect":
            try:
                info = json.loads(base64.b64decode(payload).decode())
                await self.open_connection(sid, info['addr'], info['port'])
            except Exception as e:
                logging.error(f"[{sid}] Connect parse error: {e}")
        elif p_type == "data" and payload:
            if sid in self.sessions and self.sessions[sid]['active']:
                try:
                    raw_data = base64.b64decode(payload)
                    self.sessions[sid]['writer'].write(raw_data)
                    await self.sessions[sid]['writer'].drain()
                    self.total_bytes_rx += len(raw_data)
                except Exception as e:
                    logging.debug(f"[{sid}] Write error: {e}")
                    self.sessions[sid]['active'] = False
        elif p_type == "close":
            if sid in self.sessions:
                try:
                    self.sessions[sid]['writer'].close()
                except:
                    pass
                del self.sessions[sid]
                return {"sid": sid, "data": "CLOSED", "type": "close"}

        if sid in self.sessions:
            async with self.buffer_lock:
                self.sessions[sid]['last_poll'] = time.time()
                if self.sessions[sid]['buffer']:
                    resp_data = base64.b64encode(self.sessions[sid]['buffer']).decode()
                    self.total_bytes_tx += len(self.sessions[sid]['buffer'])
                    self.sessions[sid]['buffer'] = bytearray()

        return {"sid": sid, "data": resp_data, "type": p_type}

    async def open_connection(self, sid, addr, port):
        """Открытие исходящего TCP соединения во внешний интернет с хоста Render"""
        try:
            logging.info(f"[*] [{sid}] Connecting to {addr}:{port}...")
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(addr, port),
                timeout=12.0
            )
            self.sessions[sid] = {
                'writer': writer,
                'reader': reader,
                'buffer': bytearray(),
                'active': True,
                'last_poll': time.time()
            }
            asyncio.create_task(self.read_remote(sid, reader))
            logging.info(f"[+] [{sid}] Connected to {addr}:{port}")
        except Exception as e:
            logging.error(f"[-] [{sid}] Connection failed ({addr}:{port}): {e}")

    async def read_remote(self, sid, reader):
        """Чтение данных от целевого хоста во внутренний буфер сессии"""
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
        """Очистка заброшенных или закрытых сессий"""
        while True:
            await asyncio.sleep(15)
            now = time.time()
            to_del = []
            for sid, s in list(self.sessions.items()):
                # Если сессия неактивна и буфер пуст, либо не опрашивалась > 45 сек
                if (not s['active'] and len(s['buffer']) == 0) or (now - s['last_poll'] > 45):
                    to_del.append(sid)

            for sid in to_del:
                if sid in self.sessions:
                    try:
                        self.sessions[sid]['writer'].close()
                    except:
                        pass
                    del self.sessions[sid]
                    logging.info(f"[X] Cleaned session {sid}")

    async def run(self):
        app = web.Application()
        app.router.add_get('/', self.get_dashboard)
        app.router.add_get('/healthz', self.handle_ping)
        app.router.add_get('/api/ping', self.handle_ping)
        app.router.add_post('/api/batch', self.handle_batch)
        # Обработка любых других путей (для совместимости)
        app.router.add_get('/{tail:.*}', self.get_dashboard)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', LISTEN_PORT)
        await site.start()

        logging.info(f"==================================================")
        logging.info(f"🚀 RENDER BRIDGE SERVER STARTED ON PORT {LISTEN_PORT}")
        logging.info(f"==================================================")
        await self.session_cleaner()


if __name__ == "__main__":
    try:
        asyncio.run(YandexRenderBridgeServer().run())
    except KeyboardInterrupt:
        pass
