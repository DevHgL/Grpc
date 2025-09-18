import asyncio
import grpc
import json
from generated import pubsub_pb2, pubsub_pb2_grpc


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

    # fallback → ignora eventos que não precisam floodar
    return None


async def run(topic: str, client_id: str, target="localhost:50051"):
    async with grpc.aio.insecure_channel(target) as channel:
        stub = pubsub_pb2_grpc.PubSubStub(channel)
        req = pubsub_pb2.SubscribeRequest(topic=topic, client_id=client_id)

        print(f"[{client_id}] Assinando '{topic}' em {target}...")
        try:
            async for msg in stub.Subscribe(req):
                try:
                    event = json.loads(msg.content)
                except Exception:
                    continue

                formatted = format_event(event)
                if formatted:   # só exibe se tiver saída formatada
                    print(f"[{client_id}] {formatted}")
        except grpc.aio.AioRpcError as e:
            print(f"[{client_id}] stream encerrado: {e}")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("topic", help="Tópico (ex: brasileirao/copa/live)")
    p.add_argument("client_id", help="ID do cliente")
    p.add_argument("--target", default="localhost:50051")
    args = p.parse_args()

    asyncio.run(run(args.topic, args.client_id, args.target))
