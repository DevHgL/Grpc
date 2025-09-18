import asyncio
import grpc
import json
import random
import time

from generated import pubsub_pb2, pubsub_pb2_grpc

# ⚽ 12 maiores clubes do Brasil
TEAMS = [
    "Flamengo", "Palmeiras", "São Paulo", "Corinthians",
    "Fluminense", "Vasco", "Botafogo", "Grêmio",
    "Internacional", "Athletico-PR", "Atlético-MG", "Cruzeiro"
]


async def publish_event(stub, topic, payload):
    """Publica em um tópico específico."""
    req = pubsub_pb2.PublishRequest(
        topic=topic,
        content=json.dumps(payload),
        client_id="cup_simulator"
    )
    await stub.Publish(req)
    print(f"[cup] {payload}")


async def publish_both(stub, match_topic, payload):
    """Publica no tópico da partida e no tópico geral da copa."""
    await publish_event(stub, match_topic, payload)
    await publish_event(stub, "brasileirao/copa/live", payload)


async def simulate_match(stub, match_id, team1, team2, round_name):
    topic = f"brasileirao/copa/match/{match_id}"
    score = {team1: 0, team2: 0}

    # Início da partida
    start_event = {
        "type": "MATCH_START",
        "round": round_name,
        "fixture": f"{team1} x {team2}",
        "timestamp": time.time()
    }
    await publish_both(stub, topic, start_event)

    # 90 minutos (simulação rápida)
    for minute in range(1, 91):
        await asyncio.sleep(0.05)  # acelera
        if random.random() < 0.07:  # ~7% chance por minuto
            team = random.choice([team1, team2])
            score[team] += 1
            goal_event = {
                "type": "GOAL",
                "minute": minute,
                "scorer_team": team,
                "score": f"{score[team1]} - {score[team2]}"
            }
            await publish_both(stub, topic, goal_event)

    # Desempate por pênaltis, se necessário
    if score[team1] == score[team2]:
        pen_start = {
            "type": "PENALTIES_START",
            "fixture": f"{team1} x {team2}"
        }
        await publish_both(stub, topic, pen_start)

        while score[team1] == score[team2]:
            team = random.choice([team1, team2])
            score[team] += 1
            pen_goal = {
                "type": "PENALTY_GOAL",
                "team": team,
                "score": f"{score[team1]} - {score[team2]}"
            }
            await publish_both(stub, topic, pen_goal)

    # Fim de jogo
    winner = team1 if score[team1] > score[team2] else team2
    end_event = {
        "type": "MATCH_END",
        "round": round_name,
        "fixture": f"{team1} x {team2}",
        "final_score": f"{score[team1]} - {score[team2]}",
        "winner": winner
    }
    await publish_both(stub, topic, end_event)

    return winner


async def play_round(stub, teams, round_name, match_id_start=1):
    random.shuffle(teams)
    winners = []
    for i in range(0, len(teams), 2):
        team1, team2 = teams[i], teams[i + 1]
        winner = await simulate_match(stub, match_id_start + i // 2, team1, team2, round_name)
        winners.append(winner)
    return winners


async def main(target="localhost:50051"):
    async with grpc.aio.insecure_channel(target) as channel:
        stub = pubsub_pb2_grpc.PubSubStub(channel)

        # 12 times → primeira fase com 4 jogos (8 times) + 4 avançam direto = 8 nas quartas
        prelim = TEAMS[:8]  # 8 jogam a fase preliminar
        rest = TEAMS[8:]    # 4 já avançam

        prelim_winners = await play_round(stub, prelim, "PRELIM")
        teams8 = rest + prelim_winners

        # quartas
        winners4 = await play_round(stub, teams8, "QUARTER_FINALS")
        # semis
        winners2 = await play_round(stub, winners4, "SEMI_FINALS")
        # final
        champion = await play_round(stub, winners2, "FINAL")

        end_event = {
            "type": "TOURNAMENT_END",
            "champion": champion[0]
        }
        await publish_event(stub, "brasileirao/copa/live", end_event)


if __name__ == "__main__":
    asyncio.run(main())
