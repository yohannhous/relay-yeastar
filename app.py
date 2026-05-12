import os
import asyncio
import json
import base64
import audioop
import websockets
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
SYSTEM_PROMPT = os.environ.get(
    "SYSTEM_PROMPT",
    "Tu es le réceptionniste vocal de Ma Boutique Réunion. "
    "Tu es chaleureux, poli et concis. "
    "Tu réponds toujours en 1 ou 2 phrases maximum. "
    "Tu renseignes sur les horaires, les produits et les promotions. "
    "Si le client veut parler à quelqu'un, dis-lui de taper 0.",
)

OPENAI_WS_URL = (
    "wss://api.openai.com/v1/realtime"
    "?model=gpt-4o-realtime-preview-2024-12-17"
)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/")
async def root():
    return {"message": "Relay Yeastar → OpenAI Realtime opérationnel"}


@app.websocket("/media")
async def media_ws(client_ws: WebSocket):
    await client_ws.accept()
    print("📞 Appel entrant — connexion Yeastar établie")

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

            async def yeastar_to_openai():
                try:
                    async for message in client_ws.iter_bytes():
                        pcm8 = audioop.ulaw2lin(message, 2)
                        pcm24 = audioop.ratecv(pcm8, 2, 1, 8000, 24000, None)[0]
                        b64 = base64.b64encode(pcm24).decode("utf-8")
                        await openai_ws.send(json.dumps({
                            "type": "input_audio_buffer.append",
                            "audio": b64,
                        }))
                except WebSocketDisconnect:
                    print("📵 Yeastar déconnecté")

            async def openai_to_yeastar():
                try:
                    async for raw in openai_ws:
                        event = json.loads(raw)

                        if event.get("type") == "response.audio.delta":
                            pcm24 = base64.b64decode(event["delta"])
                            pcm8 = audioop.ratecv(pcm24, 2, 1, 24000, 8000, None)[0]
                            ulaw = audioop.lin2ulaw(pcm8, 2)
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
        print(f"❌ Erreur connexion OpenAI : {e}")

    print("📞 Appel terminé")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("app:app", host="0.0.0.0", port=port, log_level="info")
