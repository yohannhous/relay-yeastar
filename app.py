import os
import asyncio
import json
import base64
import audioop
import threading
import socket
import hashlib
import re
import time
import random
import struct
import websockets
import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager

# ── Variables d'environnement ────────────────────────────────────────────────
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
SIP_PORT      = int(os.environ.get("SIP_PORT", "5060"))
RTP_PORT_BASE = int(os.environ.get("RTP_PORT_BASE", "20000"))

OPENAI_WS_URL = (
    "wss://api.openai.com/v1/realtime"
    "?model=gpt-4o-realtime-preview-2024-12-17"
)

# ── Utilitaires SIP ──────────────────────────────────────────────────────────
def md5h(s):
    return hashlib.md5(s.encode()).hexdigest()

def parse_header(msg, header):
    m = re.search(rf'^{header}\s*:\s*(.+)$', msg, re.MULTILINE | re.IGNORECASE)
    return m.group(1).strip() if m else ""

def build_response(request, code, reason, extra_headers="", body=""):
    via     = parse_header(request, "Via")
    from_h  = parse_header(request, "From")
    to_h    = parse_header(request, "To")
    call_id = parse_header(request, "Call-ID")
    cseq    = parse_header(request, "CSeq")
    content_type = f"Content-Type: application/sdp\r\n" if body else ""
    return (
        f"SIP/2.0 {code} {reason}\r\n"
        f"Via: {via}\r\n"
        f"From: {from_h}\r\n"
        f"To: {to_h}\r\n"
        f"Call-ID: {call_id}\r\n"
        f"CSeq: {cseq}\r\n"
        f"Contact: <sip:{SIP_EXTENSION}@{SIP_SERVER}:{SIP_PORT}>\r\n"
        f"{extra_headers}"
        f"{content_type}"
        f"Content-Length: {len(body)}\r\n"
        f"\r\n"
        f"{body}"
    )

def build_sdp(rtp_port, local_ip):
    return (
        "v=0\r\n"
        f"o=- {int(time.time())} {int(time.time())} IN IP4 {local_ip}\r\n"
        "s=ReceptionnisteIA\r\n"
        f"c=IN IP4 {local_ip}\r\n"
        "t=0 0\r\n"
        f"m=audio {rtp_port} RTP/AVP 0\r\n"
        "a=rtpmap:0 PCMU/8000\r\n"
        "a=sendrecv\r\n"
    )

def parse_sdp_port(sdp):
    m = re.search(r'm=audio\s+(\d+)', sdp)
    return int(m.group(1)) if m else 10000

def parse_sdp_ip(sdp, fallback):
    m = re.search(r'c=IN IP4 ([\d.]+)', sdp)
    return m.group(1) if m else fallback

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((SIP_SERVER, SIP_PORT))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "0.0.0.0"

def build_rtp_packet(seq, ts, ssrc, payload):
    header = struct.pack('!BBHII', 0x80, 0x00, seq, ts, ssrc)
    return header + payload

def parse_rtp_payload(data):
    if len(data) < 12:
        return b""
    return data[12:]

# ── Pont OpenAI Realtime ────────────────────────────────────────────────────
async def openai_bridge(rtp_sock, remote_ip, remote_port):
    """Pont bidirectionnel RTP ↔ OpenAI Realtime"""
    print(f"🤖 Connexion OpenAI Realtime...")
    ssrc = random.randint(0, 0xFFFFFFFF)
    seq  = 0
    ts   = 0

    try:
        async with websockets.connect(
            OPENAI_WS_URL,
            additional_headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        ) as openai_ws:

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
            print("✅ Session OpenAI configurée — en écoute")

            loop = asyncio.get_event_loop()

            async def rtp_to_openai():
                """RTP entrant (G.711 µ-law) → OpenAI PCM16"""
                while True:
                    try:
                        data = await loop.run_in_executor(None, lambda: rtp_sock.recv(4096))
                        payload = parse_rtp_payload(data)
                        if not payload:
                            continue
                        pcm8  = audioop.ulaw2lin(payload, 2)
                        pcm24 = audioop.ratecv(pcm8, 2, 1, 8000, 24000, None)[0]
                        b64   = base64.b64encode(pcm24).decode("utf-8")
                        await openai_ws.send(json.dumps({
                            "type": "input_audio_buffer.append",
                            "audio": b64,
                        }))
                    except Exception:
                        break

            async def openai_to_rtp():
                """OpenAI PCM16 → RTP sortant (G.711 µ-law)"""
                nonlocal seq, ts
                try:
                    async for raw in openai_ws:
                        event = json.loads(raw)

                        if event.get("type") == "response.audio.delta":
                            pcm24   = base64.b64decode(event["delta"])
                            pcm8    = audioop.ratecv(pcm24, 2, 1, 24000, 8000, None)[0]
                            ulaw    = audioop.lin2ulaw(pcm8, 2)
                            # Envoie en paquets RTP de 160 bytes (20ms à 8kHz)
                            for i in range(0, len(ulaw), 160):
                                chunk  = ulaw[i:i+160]
                                pkt    = build_rtp_packet(seq & 0xFFFF, ts & 0xFFFFFFFF, ssrc, chunk)
                                rtp_sock.sendto(pkt, (remote_ip, remote_port))
                                seq += 1
                                ts  += 160

                        elif event.get("type") == "response.audio_transcript.delta":
                            print(f"🤖 {event.get('delta', '')}", end="", flush=True)

                        elif event.get("type") == "conversation.item.input_audio_transcription.completed":
                            print(f"\n👤 Client : {event.get('transcript', '')}")

                        elif event.get("type") == "error":
                            print(f"\n❌ OpenAI erreur : {event}")

                except Exception as e:
                    print(f"OpenAI WS erreur : {e}")

            await asyncio.gather(rtp_to_openai(), openai_to_rtp())

    except Exception as e:
        print(f"❌ Connexion OpenAI échouée : {e}")

# ── Serveur SIP UDP ──────────────────────────────────────────────────────────
def sip_server_thread():
    """Écoute les messages SIP entrants et répond aux INVITE"""
    sip_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sip_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sip_sock.bind(("0.0.0.0", SIP_PORT))
    print(f"📡 Serveur SIP en écoute sur le port {SIP_PORT}")

    local_ip  = get_local_ip()
    rtp_port  = RTP_PORT_BASE
    call_loop = asyncio.new_event_loop()

    while True:
        try:
            data, addr = sip_sock.recvfrom(65535)
            msg = data.decode(errors="ignore")
            first_line = msg.split("\r\n")[0]

            # ── REGISTER ──
            if msg.startswith("REGISTER"):
                resp = build_response(msg, 200, "OK",
                    extra_headers=f"Expires: 300\r\nDate: {time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime())}\r\n")
                sip_sock.sendto(resp.encode(), addr)

            # ── INVITE ──
            elif msg.startswith("INVITE"):
                print(f"📞 INVITE reçu de {addr[0]}:{addr[1]}")

                # 100 Trying
                trying = build_response(msg, 100, "Trying")
                sip_sock.sendto(trying.encode(), addr)

                # Analyse SDP de l'appelant
                sdp_part    = msg.split("\r\n\r\n", 1)[-1]
                remote_rtp_port = parse_sdp_port(sdp_part)
                remote_rtp_ip   = parse_sdp_ip(sdp_part, addr[0])

                # 180 Ringing
                ringing = build_response(msg, 180, "Ringing")
                sip_sock.sendto(ringing.encode(), addr)
                time.sleep(0.5)

                # Ouvre socket RTP local
                rtp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                rtp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                rtp_sock.bind(("0.0.0.0", rtp_port))
                rtp_sock.settimeout(30)

                # 200 OK avec SDP
                sdp  = build_sdp(rtp_port, local_ip)
                ok   = build_response(msg, 200, "OK", body=sdp)
                sip_sock.sendto(ok.encode(), addr)
                print(f"✅ Appel accepté — RTP local:{rtp_port} → distant:{remote_rtp_ip}:{remote_rtp_port}")

                # Lance le pont OpenAI dans un thread asyncio séparé
                def run_bridge():
                    asyncio.run(openai_bridge(rtp_sock, remote_rtp_ip, remote_rtp_port))
                    rtp_sock.close()
                    print("📞 Appel terminé — RTP fermé")

                t = threading.Thread(target=run_bridge, daemon=True)
                t.start()

                rtp_port += 2  # prochain appel sur le port suivant

            # ── ACK ──
            elif msg.startswith("ACK"):
                pass  # rien à faire

            # ── BYE ──
            elif msg.startswith("BYE"):
                print("📵 BYE reçu — appel raccroché")
                resp = build_response(msg, 200, "OK")
                sip_sock.sendto(resp.encode(), addr)

            # ── OPTIONS (keepalive) ──
            elif msg.startswith("OPTIONS"):
                resp = build_response(msg, 200, "OK")
                sip_sock.sendto(resp.encode(), addr)

        except Exception as e:
            print(f"⚠️ SIP erreur : {e}")

# ── Enregistrement SIP vers Yeastar ─────────────────────────────────────────
def sip_registration_loop():
    if not (SIP_SERVER and SIP_USER and SIP_PASS):
        print("⚠️ Variables SIP manquantes")
        return

    port    = SIP_PORT
    call_id = f"{os.urandom(8).hex()}@{SIP_SERVER}"

    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(5)
            addr = (SIP_SERVER, port)

            via_branch = f"z9hG4bK{os.urandom(4).hex()}"
            tag        = os.urandom(4).hex()

            def make_register(cseq, auth_header=""):
                msg = (
                    f"REGISTER sip:{SIP_SERVER} SIP/2.0\r\n"
                    f"Via: SIP/2.0/UDP {SIP_SERVER}:{port};branch={via_branch}\r\n"
                    f"From: <sip:{SIP_EXTENSION}@{SIP_SERVER}>;tag={tag}\r\n"
                    f"To: <sip:{SIP_EXTENSION}@{SIP_SERVER}>\r\n"
                    f"Call-ID: {call_id}\r\n"
                    f"CSeq: {cseq} REGISTER\r\n"
                    f"Contact: <sip:{SIP_EXTENSION}@{SIP_SERVER}:{port}>\r\n"
                    f"Expires: 300\r\n"
                    f"Max-Forwards: 70\r\n"
                    f"User-Agent: YeastarRelay/1.0\r\n"
                )
                if auth_header:
                    msg += f"Authorization: {auth_header}\r\n"
                msg += "Content-Length: 0\r\n\r\n"
                return msg

            sock.sendto(make_register(1).encode(), addr)
            resp1, _ = sock.recvfrom(4096)
            resp1 = resp1.decode(errors="ignore")

            if "401" in resp1 or "407" in resp1:
                realm_m = re.search(r'realm="([^"]+)"', resp1)
                nonce_m = re.search(r'nonce="([^"]+)"', resp1)
                realm   = realm_m.group(1) if realm_m else ""
                nonce   = nonce_m.group(1) if nonce_m else ""
                ha1     = md5h(f"{SIP_USER}:{realm}:{SIP_PASS}")
                ha2     = md5h(f"REGISTER:sip:{SIP_SERVER}")
                res     = md5h(f"{ha1}:{nonce}:{ha2}")
                auth    = (f'Digest username="{SIP_USER}",realm="{realm}",'
                           f'nonce="{nonce}",uri="sip:{SIP_SERVER}",'
                           f'response="{res}",algorithm=MD5')
                sock.sendto(make_register(2, auth).encode(), addr)
                resp2, _ = sock.recvfrom(4096)
                resp2 = resp2.decode(errors="ignore")
                if "200 OK" in resp2:
                    print(f"✅ SIP enregistré : {SIP_EXTENSION}@{SIP_SERVER}")
                else:
                    print(f"⚠️ SIP échec : {resp2[:80]}")
            elif "200 OK" in resp1:
                print(f"✅ SIP enregistré : {SIP_EXTENSION}@{SIP_SERVER}")
            else:
                print(f"⚠️ SIP inattendu : {resp1[:80]}")

            sock.close()
        except Exception as e:
            print(f"⚠️ SIP registration erreur : {e}")

        time.sleep(240)

# ── FastAPI ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Lance le serveur SIP
    t1 = threading.Thread(target=sip_server_thread, daemon=True)
    t1.start()
    # Lance l'enregistrement SIP
    t2 = threading.Thread(target=sip_registration_loop, daemon=True)
    t2.start()
    print("🚀 Relay démarré — SIP server + registration lancés")
    yield
    print("🛑 Relay arrêté")

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/")
async def root():
    return {
        "status": "opérationnel",
        "sip_server": SIP_SERVER,
        "extension": SIP_EXTENSION,
        "sip_port": SIP_PORT,
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("app:app", host="0.0.0.0", port=port, log_level="info")
