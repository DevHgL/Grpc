import asyncio
import grpc
import json
import random
import time

from generated import pubsub_pb2, pubsub_pb2_grpc

TEAMS = [
    "Flamengo", "Palmeiras", "São Paulo", "Corinthians", "Fluminense",
    "Vasco", "Botafogo", "Grêmio", "Internacional", "Athletico-PR",
    "Atlético-MG", "Cruzeiro", "Fortaleza", "Ceará", "Bahia",
    "Red Bull Bragantino", "América-MG", "Cuiabá", "Juventude", "Coritiba"
]


async def publish_event(stub, topic, payload):
    req = pubsub_pb2.PublishRequest(
        topic=topic,
        content=json.dumps(payload),
        client_id="cup_simulator"
    )
    await stub.Publish(req)
    print(f"[cup] {payload}")


async def simulate_match(stub, match_id, team1, team2, round_name):
    topic = f"brasileirao/copa/match/{match_id}"
    score = {team1: 0, team2: 0}

    await publish_event(stub, topic, {
        "type": "MATCH_START",
        "round": round_name,
        "fixture": f"{team1} x {team2}",
        "timestamp": time.time()
    })

    # 90 minutos (simulado com 1s = 1min)
    for minute in range(1, 91):
        await asyncio.sleep(0.05)  # mais rápido
        if random.random() < 0.07:  # ~7% chance de gol por minuto
            team = random.choice([team1, team2])
            score[team] += 1
            await publish_event(stub, topic, {
                "type": "GOAL",
                "minute": minute,
                "scorer_team": team,
                "score": f"{score[team1]} - {score[team2]}"
            })

    # empate → pênaltis
    if score[team1] == score[team2]:
        await publish_event(stub, topic, {
            "type": "PENALTIES_START",
            "fixture": f"{team1} x {team2}"
        })
        while score[team1] == score[team2]:
            team = random.choice([team1, team2])
            score[team] += 1
            await publish_event(stub, topic, {
                "type": "PENALTY_GOAL",
                "team": team,
                "score": f"{score[team1]} - {score[team2]}"
            })

    # fim
    winner = team1 if score[team1] > score[team2] else team2
    await publish_event(stub, topic, {
        "type": "MATCH_END",
        "round": round_name,
        "fixture": f"{team1} x {team2}",
        "final_score": f"{score[team1]} - {score[team2]}",
        "winner": winner
    })

    return winner


async def play_round(stub, teams, round_name, match_id_start=1):
    random.shuffle(teams)
    winners = []
    tasks = []
    for i in range(0, len(teams), 2):
        team1, team2 = teams[i], teams[i+1]
        winner = await simulate_match(stub, match_id_start+i//2, team1, team2, round_name)
        winners.append(winner)
    return winners


async def main(target="localhost:50051"):
    async with grpc.aio.insecure_channel(target) as channel:
        stub = pubsub_pb2_grpc.PubSubStub(channel)

        # fase preliminar (4 jogos → 8 times → 16 times restantes)
        prelim = TEAMS[:8]  # 8 times jogam eliminatória
        rest = TEAMS[8:]    # 12 avançam direto
        prelim_winners = await play_round(stub, prelim, "PRELIM")
        teams16 = rest + prelim_winners

        # oitavas
        winners8 = await play_round(stub, teams16, "ROUND_OF_16")
        # quartas
        winners4 = await play_round(stub, winners8, "QUARTER_FINALS")
        # semis
        winners2 = await play_round(stub, winners4, "SEMI_FINALS")
        # final
        champion = await play_round(stub, winners2, "FINAL")

        await publish_event(stub, "brasileirao/copa/live", {
            "type": "TOURNAMENT_END",
            "champion": champion[0]
        })


if __name__ == "__main__":
    asyncio.run(main())
