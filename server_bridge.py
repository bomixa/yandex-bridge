import asyncio
import base64
import json
import logging
import os
import sys
import time
import uuid
from aiohttp import web, ClientSession, ClientTimeout

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.json")

DEFAULT_CONFIG = {
    "render_url": "https://yandex-bridge-dlc6.onrender.com",
    "socks5_port": 52090,
    "http_port": 58090,
    "local_api_port": 8888,
    "keepalive_interval_sec": 60
}

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
                return {**DEFAULT_CONFIG, **cfg}
        except Exception as e:
            logging.warning(f"Не удалось прочитать config.json ({e})")
    return DEFAULT_CONFIG

config = load_config()
SOCKS5_PORT = int(config["socks5_port"])
HTTP_PORT = int(config["http_port"])
LOCAL_API_PORT = int(config["local_api_port"])
RENDER_URL = config["render_url"].rstrip('/')
KEEPALIVE_INTERVAL = int(config["keepalive_interval_sec"])
CHUNK_SIZE = 16384

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logging.getLogger('aiohttp.access').setLevel(logging.WARNING)


class YandexTurboClient:
    def __init__(self):
        self.sessions = {}  # sid -> writer
        self.outgoing_queue = asyncio.Queue()
        self.is_connected_to_yandex = False
        self.total_rx = 0
        self.total_tx = 0

    async def handle_api(self, request):
        """API эндпоинт для браузерного моста"""
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Private-Network': 'true'
        }
        if request.method == "OPTIONS":
            return web.Response(headers=headers)

        if not self.is_connected_to_yandex:
            self.is_connected_to_yandex = True
            logging.info("🔗 [МОСТ] Браузер подключился к локальному клиенту!")

        # 1. Принимаем входящие ответы от сервера
        incoming = []
        try:
            if request.method == "POST":
                incoming = await request.json()
        except Exception:
            pass

        if incoming and isinstance(incoming, list):
            for res in incoming:
                sid = res.get("sid")
                data_b64 = res.get("data")

                if sid in self.sessions and data_b64:
                    if data_b64 == "EOF":
                        try:
                            self.sessions[sid].close()
                        except:
                            pass
                        if sid in self.sessions:
                            del self.sessions[sid]
                    elif data_b64 not in ["EMPTY", "OK", "ERROR"]:
                        try:
                            raw = base64.b64decode(data_b64)
                            if raw:
                                self.sessions[sid].write(raw)
                                await self.sessions[sid].drain()
                                self.total_rx += len(raw)
                                logging.info(f"[<---] [{sid}] Получено: {len(raw)} байт")
                        except Exception as e:
                            logging.debug(f"Write error [{sid}]: {e}")
                            if sid in self.sessions:
                                del self.sessions[sid]

        # 2. Формируем пачку задач (Срочные connect/data идут первыми!)
        tasks = []
        while not self.outgoing_queue.empty():
            tasks.append(await self.outgoing_queue.get())

        for sid in list(self.sessions.keys()):
            tasks.append({"sid": sid, "t": "poll", "p": ""})

        if not tasks:
            tasks.append({"sid": "IDLE", "t": "handshake", "p": ""})

        return web.json_response(tasks, headers=headers)

    async def handle_proxy(self, reader, writer, proto):
        """Обработка клиентских подключений SOCKS5 / HTTP CONNECT"""
        sid = str(uuid.uuid4())[:8]
        addr = ""
        port = 0
        initial_data = b""

        try:
            if proto == "HTTP":
                line = await reader.readline()
                if not line:
                    writer.close()
                    return
                if b"CONNECT" in line:
                    target = line.decode('utf-8', 'ignore').split()[1]
                    addr, port = target.split(":") if ":" in target else (target, 443)
                    port = int(port)
                    writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    await writer.drain()
                else:
                    initial_data = line
                    while True:
                        l = await reader.readline()
                        initial_data += l
                        if l.strip() == b"":
                            break
                        if l.lower().startswith(b"host:"):
                            host_str = l.decode('utf-8', 'ignore').split(":")[1].strip()
                            addr = host_str
                    port = 80
                    if not addr:
                        writer.close()
                        return
            else:  # SOCKS5
                header = await reader.read(2)
                if not header or header[0] != 0x05:
                    writer.close()
                    return
                nmethods = header[1]
                await reader.read(nmethods)
                writer.write(bytes([0x05, 0x00]))
                await writer.drain()

                request = await reader.read(4)
                if not request or request[1] != 0x01:
                    writer.close()
                    return
                atyp = request[3]

                if atyp == 0x01:
                    addr_raw = await reader.read(4)
                    addr = ".".join(map(str, addr_raw))
                elif atyp == 0x03:
                    addr_len = (await reader.read(1))[0]
                    addr = (await reader.read(addr_len)).decode('utf-8', 'ignore')
                else:
                    writer.close()
                    return

                port_raw = await reader.read(2)
                port = int.from_bytes(port_raw, 'big')

                writer.write(bytes([0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0]))
                await writer.drain()

            addr = str(addr).strip(" \t\n\r\x00/\"'")
            
            # Исключения: не проксируем внутренние адреса
            if addr in ["127.0.0.1", "localhost"] or "onrender.com" in addr or "yandex." in addr or "turbopages.org" in addr:
                writer.close()
                return

            logging.info(f"[--->] [{proto}][{sid}] Запрос туннеля -> {addr}:{port}")

            self.sessions[sid] = writer
            p = base64.b64encode(json.dumps({"addr": addr, "port": port}).encode()).decode()
            await self.outgoing_queue.put({"sid": sid, "t": "connect", "p": p})

            if initial_data:
                await self.outgoing_queue.put({
                    "sid": sid,
                    "t": "data",
                    "p": base64.b64encode(initial_data).decode()
                })

            while sid in self.sessions:
                try:
                    data = await asyncio.wait_for(reader.read(CHUNK_SIZE), timeout=0.1)
                    if not data:
                        break
                    await self.outgoing_queue.put({
                        "sid": sid,
                        "t": "data",
                        "p": base64.b64encode(data).decode()
                    })
                    self.total_tx += len(data)
                    logging.info(f"[--->] [{sid}] Передано: {len(data)} байт")
                except asyncio.TimeoutError:
                    await asyncio.sleep(0.01)
                    continue
                except (ConnectionResetError, ConnectionAbortedError):
                    break
                except Exception:
                    break

        except Exception as e:
            if not isinstance(e, (ConnectionResetError, ConnectionAbortedError)):
                logging.debug(f"Proxy error [{sid}]: {e}")
        finally:
            if sid in self.sessions:
                del self.sessions[sid]
            try:
                writer.close()
            except:
                pass
            await self.outgoing_queue.put({"sid": sid, "t": "close", "p": ""})

    async def keepalive_worker(self):
        """Фоновый пинг сервера Render для предотвращения сна"""
        ping_url = f"{RENDER_URL}/api/ping"
        timeout = ClientTimeout(total=15.0)

        while True:
            try:
                if "your-app-name" not in RENDER_URL:
                    t_start = time.time()
                    async with ClientSession(timeout=timeout) as session:
                        async with session.get(ping_url) as resp:
                            elapsed_ms = int((time.time() - t_start) * 1000)
                            if resp.status == 200:
                                data = await resp.json()
                                uptime = data.get("uptime_sec", 0)
                                active_s = data.get("active_sessions", 0)
                                logging.info(f"💚 [KEEPALIVE] Render ОНЛАЙН (ping: {elapsed_ms}ms, аптайм: {uptime}с, сессий: {active_s})")
            except Exception as e:
                logging.warning(f"⏳ [KEEPALIVE] Ожидание связи с Render: {e}")

            await asyncio.sleep(KEEPALIVE_INTERVAL)

    async def run(self):
        app = web.Application()
        app.router.add_route('*', '/exchange', self.handle_api)
        app.router.add_route('*', '/{tail:.*}', self.handle_api)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, '0.0.0.0', LOCAL_API_PORT).start()

        s5 = await asyncio.start_server(lambda r, w: self.handle_proxy(r, w, "SOCKS5"), "0.0.0.0", SOCKS5_PORT)
        hp = await asyncio.start_server(lambda r, w: self.handle_proxy(r, w, "HTTP"), "0.0.0.0", HTTP_PORT)

        asyncio.create_task(self.keepalive_worker())

        yandex_bridge_url = f"https://translate.yandex.ru/translate?url={RENDER_URL}"

        print("\n" + "="*70)
        print("  🚀 YANDEX RELAY BRIDGE v7.0 (RENDER.COM EDITION)")
        print("="*70)
        print(f"  • SOCKS5 Прокси:    127.0.0.1:{SOCKS5_PORT}")
        print(f"  • HTTP Прокси:      127.0.0.1:{HTTP_PORT}")
        print(f"  • Локальный API:    127.0.0.1:{LOCAL_API_PORT}")
        print(f"  • Сервер Render:    {RENDER_URL}")
        print(f"  • Keep-Alive:       каждые {KEEPALIVE_INTERVAL} сек.")
        print("="*70)
        print("  👉 ССЫЛКА ДЛЯ БРАУЗЕРА (ОТКРОЙТЕ В ЯНДЕКС.ПЕРЕВОДЧИКЕ):")
        print(f"  {yandex_bridge_url}")
        print("="*70)
        print("  ⚠️ ВАЖНО: В настройках прокси-плагина добавьте в исключения:")
        print("     localhost, 127.0.0.1, *.yandex.ru, *.turbopages.org, *.onrender.com")
        print("="*70 + "\n")

        async with s5, hp:
            await asyncio.gather(s5.serve_forever(), hp.serve_forever())


if __name__ == "__main__":
    try:
        asyncio.run(YandexTurboClient().run())
    except KeyboardInterrupt:
        print("\n[!] Остановка клиента...")
        sys.exit(0)
