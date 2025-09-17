# workers/simulator.py
import asyncio
import json
import random
import time
import grpc

from generated import pubsub_pb2, pubsub_pb2_grpc

TIMES = [
    "Flamengo", "Palmeiras", "Corinthians", "São Paulo",
    "Fluminense", "Vasco", "Botafogo", "Grêmio",
    "Internacional", "Athletico-PR", "Atlético-MG", "Cruzeiro"
]

async def simulate_goals(target="localhost:50051"):
    async with grpc.aio.insecure_channel(target) as channel:
        stub = pubsub_pb2_grpc.PubSubStub(channel)
        placar = {time: 0 for time in TIMES}

        print("[simulador] começando a simular gols...")

        while True:
            # sorteia 2 times para jogar
            home, away = random.sample(TIMES, 2)

            # sorteia quem marcou
            scorer = random.choice([home, away])
            placar[scorer] += 1

            event = {
                "type": "GOAL",
                "fixture": f"{home} x {away}",
                "minute": random.randint(1, 90),
                "scorer_team": scorer,
                "score": f"{placar[home]} - {placar[away]}"
            }

            req = pubsub_pb2.PublishRequest(
                topic="brasileirao/serie-a/live",
                content=json.dumps(event),
                client_id="simulador"
            )

            resp = await stub.Publish(req)
            print(f"[simulador] enviado: {event} | resp: {resp.info}")

            await asyncio.sleep(random.randint(5, 15))  # espera alguns segundos


if __name__ == "__main__":
    asyncio.run(simulate_goals())

