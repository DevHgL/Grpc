

import asyncio
import grpc
import json
import threading
import time
from flask import Flask, render_template, Response
from generated import pubsub_pb2, pubsub_pb2_grpc

app = Flask(__name__)

# --- Lógica de formatação de eventos (reutilizada de clients/cup_subscriber.py) ---
def format_event(event):
    etype = event.get("type")
    if etype == "MATCH_START":
        return f"🏆 Início de {event['round']}: {event['fixture']}"
    if etype == "GOAL":
        scorer = event.get("scorer", "")
        return f"⚽ {event['minute']}' – GOL do {event['scorer_team']}! ({scorer}) Placar: {event['score']}"
    if etype == "RED_CARD":
        return f"🟥 {event['minute']}' – Cartão VERMELHO para {event['team']}"
    if etype == "YELLOW_CARD":
        return f"🟨 {event['minute']}' – Cartão AMARELO para {event['team']}"
    if etype == "HALF_TIME":
        return f"⏸ Intervalo em {event['fixture']} – Placar: {event['score']}"
    if etype == "MATCH_END":
        return f"🔔 Fim de {event['round']}: {event['fixture']} | Placar final {event['final_score']} | Vencedor: {event['winner']}"
    if etype == "PENALTIES_START":
        return f"😱 Decisão por pênaltis em {event['fixture']}!"
    if etype == "PENALTY_GOAL":
        return f"🥅 Pênalti convertido por {event['team']} – Placar: {event['score']}"
    if etype == "TOURNAMENT_END":
        return f"🏆🏆 CAMPEÃO DA COPA: {event['champion']} 🏆🏆"
    return None

# --- Fila para comunicação entre o cliente gRPC e o Flask ---
# Usamos uma lista para simplicidade, mas para produção uma fila (Queue) seria melhor
notifications_queue = []

# --- Cliente gRPC que roda em background ---
async def grpc_subscriber():
    print("[Debug] Iniciando grpc_subscriber...")
    while True:
        try:
            async with grpc.aio.insecure_channel('localhost:50051') as channel:
                stub = pubsub_pb2_grpc.PubSubStub(channel)
                request = pubsub_pb2.SubscribeRequest(topic="brasileirao/serie-a/live", client_id="frontend")
                print("[Frontend] Assinando tópico 'brasileirao/serie-a/live' no broker gRPC...")
                async for message in stub.Subscribe(request):
                    print("[Debug] Mensagem recebida do gRPC.")
                    try:
                        event = json.loads(message.content)
                        formatted_message = format_event(event)
                        if formatted_message:
                            print(f"[Debug] Adicionando à fila: {formatted_message}")
                            notifications_queue.append(formatted_message)
                    except json.JSONDecodeError:
                        print("[Debug] Erro de decodificação JSON.")
                        continue
        except grpc.aio.AioRpcError as e:
            print(f"[Frontend] Erro de conexão com gRPC, tentando novamente em 5s: {e.details()}")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[Frontend] Erro inesperado no subscriber gRPC: {e}")
            await asyncio.sleep(5)


def run_grpc_subscriber():
    asyncio.run(grpc_subscriber())

# --- Rotas Flask ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/events')
def events():
    print("[Debug] Cliente conectado ao endpoint /events.")
    def generate():
        while True:
            # Copia a fila atual e a limpa
            notifications_to_send = list(notifications_queue)
            notifications_queue.clear()
            
            # Envia todas as notificações pendentes
            for notification in notifications_to_send:
                print(f"[Debug] Enviando evento: {notification}")
                yield f"data: {notification}\n\n"

            # Dorme por um curto período para evitar busy-waiting
            time.sleep(0.5)

    return Response(generate(), mimetype='text/event-stream')

if __name__ == '__main__':
    # Inicia o cliente gRPC em uma thread separada
    grpc_thread = threading.Thread(target=run_grpc_subscriber, daemon=True)
    grpc_thread.start()
    # Inicia o servidor Flask
    app.run(debug=True, port=5000, use_reloader=False)

