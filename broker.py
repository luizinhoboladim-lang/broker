"""
broker.py — servidor WebSocket que faz a ponte entre o main.py (agente)
e o ver_agentes_app.py (app desktop).

Cada main.py que se conecta é identificado por um "nome" (nome da categoria
do Discord, ex: vboxuser-177-131-189-92). O app pode mandar comandos pra
um main específico, e o broker repassa a resposta de volta.

Autenticação via header HTTP X-Broker-Token (o query string também
funciona como fallback pra clientes antigos).

Deploy: Railway. Start command: python broker.py
"""

import os
import json
import asyncio
import logging

try:
    import websockets
except ImportError:
    raise SystemExit("Instale: pip install websockets")

BROKER_TOKEN = os.getenv("BROKER_TOKEN", "").strip()
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", os.getenv("BROKER_PORT", "8080")))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [broker] %(levelname)s %(message)s",
)
log = logging.getLogger("broker")


agentes = {}   # nome -> websocket
apps = set()   # websockets dos apps

pedidos_pendentes = {}   # request_id -> websocket do app que pediu

_contador_id = 0


def novo_request_id():
    global _contador_id
    _contador_id += 1
    return f"req-{_contador_id}"


def auth_ok(websocket) -> bool:
    if not BROKER_TOKEN:
        return True

    headers = {}
    try:
        req = getattr(websocket, "request", None)
        if req is not None:
            raw = getattr(req, "headers", None)
            if raw is not None:
                try:
                    for k, v in raw.items():
                        headers[k.lower()] = v
                except AttributeError:
                    pass
    except Exception:
        pass

    tok = headers.get("x-broker-token", "")
    if tok and tok == BROKER_TOKEN:
        return True

    try:
        path = getattr(websocket, "path", "/") or "/"
        if not path:
            path = getattr(getattr(websocket, "request", None), "path", "/") or "/"
        if "token=" in path:
            token = path.split("token=")[-1].split("&")[0]
            if token == BROKER_TOKEN:
                return True
    except Exception:
        pass

    sub = headers.get("sec-websocket-protocol", "")
    if sub and sub.split(",")[0].strip() == BROKER_TOKEN:
        return True

    return False


async def handler(websocket):
    if not auth_ok(websocket):
        log.warning("conexão rejeitada: token inválido")
        await websocket.close(code=4001, reason="token inválido")
        return

    try:
        primeira = await asyncio.wait_for(websocket.recv(), timeout=15)
    except (asyncio.TimeoutError, Exception):
        await websocket.close(code=4002, reason="sem hello")
        return

    try:
        msg = json.loads(primeira)
    except Exception:
        await websocket.close(code=4003, reason="hello inválido")
        return

    tipo = msg.get("tipo")

    if tipo == "registro":
        await handle_agente(websocket, msg)
    elif tipo == "app":
        await handle_app(websocket)
    else:
        await websocket.close(code=4004, reason="tipo desconhecido")


async def handle_agente(websocket, hello):
    nome = hello.get("nome")
    if not nome:
        await websocket.close(code=4005, reason="sem nome")
        return

    if nome in agentes:
        try:
            await agentes[nome].close(code=4006, reason="substituído")
        except Exception:
            pass

    agentes[nome] = websocket
    log.info(f"agente conectado: {nome}")

    await notificar_apps({"tipo": "agente_on", "nome": nome, "info": hello})

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            tipo = msg.get("tipo")

            if tipo == "heartbeat":
                await websocket.send(json.dumps({"tipo": "pong"}))

            elif tipo == "resposta":
                rid = msg.get("request_id")
                if not rid:
                    continue
                app_ws = pedidos_pendentes.pop(rid, None)
                if app_ws:
                    try:
                        await app_ws.send(json.dumps({
                            "tipo": "resposta",
                            "request_id": rid,
                            "dados": msg.get("dados"),
                        }))
                    except Exception as e:
                        log.warning(f"falha ao repassar resposta: {e}")

            elif tipo == "evento":
                await notificar_apps({
                    "tipo": "evento",
                    "nome": nome,
                    "dados": msg.get("dados"),
                })

    except Exception as e:
        log.info(f"agente {nome} desconectou: {e}")
    finally:
        agentes.pop(nome, None)
        await notificar_apps({"tipo": "agente_off", "nome": nome})


async def handle_app(websocket):
    apps.add(websocket)
    log.info(f"app conectado (total: {len(apps)})")

    try:
        await websocket.send(json.dumps({
            "tipo": "lista_agentes",
            "agentes": list(agentes.keys()),
        }))
    except Exception:
        pass

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            tipo = msg.get("tipo")

            if tipo == "comando":
                alvo = msg.get("alvo")
                acao = msg.get("acao")
                params = msg.get("params", {})

                # ⬇️ Usa o rid que o app mandou (fallback: gera um novo)
                rid = msg.get("request_id") or novo_request_id()

                # Se o agente não existe, responde como "resposta" (não "erro")
                # pra destravar o ev.wait() do app.
                if alvo not in agentes:
                    await websocket.send(json.dumps({
                        "tipo": "resposta",
                        "request_id": rid,
                        "dados": {"erro": f"agente '{alvo}' não conectado"},
                    }))
                    continue

                pedidos_pendentes[rid] = websocket

                try:
                    await agentes[alvo].send(json.dumps({
                        "tipo": "comando",
                        "request_id": rid,
                        "acao": acao,
                        "params": params,
                    }))
                except Exception as e:
                    pedidos_pendentes.pop(rid, None)
                    await websocket.send(json.dumps({
                        "tipo": "resposta",
                        "request_id": rid,
                        "dados": {"erro": f"falha ao enviar pro agente: {e}"},
                    }))

            elif tipo == "listar":
                await websocket.send(json.dumps({
                    "tipo": "lista_agentes",
                    "agentes": list(agentes.keys()),
                    "request_id": msg.get("request_id"),
                }))

            elif tipo == "ping":
                await websocket.send(json.dumps({"tipo": "pong"}))

    except Exception as e:
        log.info(f"app desconectou: {e}")
    finally:
        apps.discard(websocket)
        log.info(f"app desconectado (total: {len(apps)})")


async def notificar_apps(msg):
    if not apps:
        return
    raw = json.dumps(msg)
    for ws in list(apps):
        try:
            await ws.send(raw)
        except Exception:
            apps.discard(ws)


async def main():
    log.info(f"broker iniciando em ws://{HOST}:{PORT}")
    if not BROKER_TOKEN:
        log.warning("BROKER_TOKEN não configurado — broker está ABERTO")
    async with websockets.serve(
        handler, HOST, PORT,
        ping_interval=20,
        ping_timeout=20,
        max_size=5 * 1024 * 1024,   # 5 MB (proxy do Railway limita em 1 MB)
    ):
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
