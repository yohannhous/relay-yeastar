import os
import asyncio
import json
import base64
import audioop
import threading
import websockets
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# ── Variables d'environnement ───────────────────────────────────────────────
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
SYSTEM_PROMPT  = os.environ.get(
    "SYSTEM_PROMPT",
    "Tu es le réceptionniste vocal de Ma Boutique Réunion. "
    "Tu es chaleureux, poli et concis. "
    "Tu réponds toujours en 1 ou 2 phrases maximum. "
    "Tu renseignes sur les horaires, les produits et les promotions. "
    "Si le client veut parler à quelqu'un, dis-lui de taper 0.",
)
SIP_SERVER    = os.environ.get("SIP_SERVER", "")
SIP_USER      = os.environ.get("SIP_USER", "")
SIP_PASS      = os.environ.get("SIP_PASS", "")
SIP_EXTENSION = os.environ.get("SIP_EXTENSION", "900")

OPENAI_WS_URL = (
    "wss://api.openai.com/v1/realtime"
    "?model=gpt-4o-realtime-preview-2024-12-17"
)

app = FastAPI()


# ── Healthcheck ─────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/")
async def root():
    return {
        "status": "opérationnel",
        "sip_server": SIP_SERVER,
        "sip_user": SIP_USER,
        "extension": SIP_EXTENSION,
    }


# ── Enregistrement SIP vers Yeastar (thread séparé) ─────────────────────────
def start_sip_registration():
    """
    Enregistre le relay comme client SIP sur Yeastar Cloud.
    Utilise sipsimple via pjsua2 ou un simple REGISTER SIP en socket UDP.
    On utilise ici une implémentation légère avec le module sip.
    """
    import socket
    import hashlib
    import time
    import re

    server   = SIP_SERVER
    user     = SIP_USER
    password = SIP_PASS
    ext      = SIP_EXTENSION
    port     = 5060

    def md5(s):
        return hashlib.md5(s.encode()).hexdigest()

    def build_register(call_id, cseq, auth=None):
        via_branch = f"z9hG4bK{os.urandom(4).hex()}"
        tag        = os.urandom(4).hex()
        msg = (
            f"REGISTER sip:{server} SIP/2.0\r\n"
            f"Via: SIP/2.0/UDP {server}:{port};branch={via_branch}\r\n"
            f"From: <sip:{ext}@{server}>;tag={tag}\r\n"
            f"To: <sip:{ext}@{server}>\r\n"
            f"Call-ID: {call_id}\r\n"
            f"CSeq: {cseq} REGISTER\r\n"
            f"Contact: <sip:{ext}@{server}:{port}>\r\n"
            f"Expires: 300\r\n"
            f"Max-Forwards: 70\r\n"
            f"User-Agent: YeastarRelay/1.0\r\n"
        )
        if auth:
            msg += f"Authorization: {auth}\r\n"
        msg += "Content-Length: 0\r\n\r\n"
        return msg

    def parse_www_auth(response):
        realm  = re.search(r'realm="([^"]+)"', response)
        nonce  = re.search(r'nonce="([^"]+)"', response)
        return (
            realm.group(1) if realm else "",
            nonce.group(1) if nonce else "",
        )

    def build_auth(realm, nonce, method="REGISTER"):
        ha1    = md5(f"{user}:{realm}:{password}")
        ha2    = md5(f"{method}:sip:{server}")
        res    = md5(f"{ha1}:{nonce}:{ha2}")
        return (
            f'Digest username="{user}",realm="{realm}",'
            f'nonce="{nonce}",uri="sip:{server}",'
            f'response="{res}",algorithm=MD5'
        )

    call_id = f"{os.urandom(8).hex()}@{server}"

    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(5)
            addr = (server, port)

            # Étape 1 : REGISTER sans auth
            msg1 = build_register(call_id, 1)
            sock.sendto(msg1.encode(), addr)
            resp1, _ = sock.recvfrom(4096)
            resp1 = resp1.decode(errors="ignore")

            if "401" in resp1 or "407" in resp1:
                realm, nonce = parse_www_auth(resp1)
                auth_header  = build_auth(realm, nonce)
                msg2 = build_register(call_id, 2, auth=auth_header)
                sock.sendto(msg2.encode(), addr)
                resp2, _ = sock.recvfrom(4096)
                resp2 = resp2.decode(errors="ignore")
                if "200 OK" in resp2:
                    print(f"✅ SIP enregistré : {ext}@{server}")
                else:
                    print(f"⚠️ SIP échec auth : {resp2[:80]}")
            elif "200 OK" in resp1:
                print(f"✅ SIP enregistré : {ext}@{server}")
            else:
                print(f"⚠️ SIP réponse inattendue : {resp1[:80]}")

            sock.close()

        except Exception as e:
            print(f"⚠️ SIP registration erreur : {e}")

        # Re-enregistrement toutes les 4 minutes (expire=300s)
        time.sleep(240)


# ── WebSocket media : Yeastar → OpenAI Realtime ─────────────────────────────
@app.websocket("/media")
async def media_ws(client_ws: WebSocket):
    await client_ws.accept()
    print("📞 Appel entrant — audio reçu de Yeastar")

    try:
        async with websockets.connect(
            OPENAI_WS_URL,
            additional_headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        ) as openai_ws:

            # Configuration session OpenAI Realtime
            await openai_ws.send(json.dumps({
                "type": "session.update",
                "session": {
                    "modalities": ["audio", "text"],
                    "instructions": SYSTEM_PROMPT,
                    "voice": "shimmer",
                    "input_audio_format": "pcm16",
                    "output_audio_format": "pcm16",
                    "input_audio_transcription": {
                        "model": "whisper-1",
                        "language": "fr",
                    },
                    "turn_detection": {
                        "type": "server_vad",
                        "threshold": 0.5,
                        "prefix_padding_ms": 300,
                        "silence_duration_ms": 700,
                    },
                    "temperature": 0.7,
                    "max_response_output_tokens": 150,
                },
            }))

            async def yeastar_to_openai():
                """Audio entrant : Yeastar G.711 µ-law 8kHz → OpenAI PCM16 24kHz"""
                try:
                    async for message in client_ws.iter_bytes():
                        pcm8  = audioop.ulaw2lin(message, 2)
                        pcm24 = audioop.ratecv(pcm8, 2, 1, 8000, 24000, None)[0]
                        b64   = base64.b64encode(pcm24).decode("utf-8")
                        await openai_ws.send(json.dumps({
                            "type": "input_audio_buffer.append",
                            "audio": b64,
                        }))
                except WebSocketDisconnect:
                    print("📵 Yeastar déconnecté")

            async def openai_to_yeastar():
                """Audio sortant : OpenAI PCM16 24kHz → Yeastar G.711 µ-law 8kHz"""
                try:
                    async for raw in openai_ws:
                        event = json.loads(raw)

                        if event.get("type") == "response.audio.delta":
                            pcm24 = base64.b64decode(event["delta"])
                            pcm8  = audioop.ratecv(pcm24, 2, 1, 24000, 8000, None)[0]
                            ulaw  = audioop.lin2ulaw(pcm8, 2)
                            await client_ws.send_bytes(ulaw)

                        elif event.get("type") == "response.audio_transcript.delta":
                            print(f"🤖 {event.get('delta', '')}", end="", flush=True)

                        elif event.get("type") == "conversation.item.input_audio_transcription.completed":
                            print(f"\n👤 Client : {event.get('transcript', '')}")

                        elif event.get("type") == "error":
                            print(f"\n❌ Erreur OpenAI : {event}")

                except Exception as e:
                    print(f"Erreur OpenAI WS : {e}")

            await asyncio.gather(yeastar_to_openai(), openai_to_yeastar())

    except Exception as e:
        print(f"❌ Erreur connexion : {e}")

    print("📞 Appel terminé")


# ── Démarrage ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Lance l'enregistrement SIP en arrière-plan
    if SIP_SERVER and SIP_USER and SIP_PASS:
        sip_thread = threading.Thread(target=start_sip_registration, daemon=True)
        sip_thread.start()
    else:
        print("⚠️ Variables SIP manquantes — enregistrement SIP désactivé")

    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("app:app", host="0.0.0.0", port=port, log_level="info")
