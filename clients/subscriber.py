import asyncio
import grpc
from generated import pubsub_pb2, pubsub_pb2_grpc


async def run(topic: str, client_id: str, target: str = "localhost:50051"):
     async with grpc.aio.insecure_channel(target) as channel:
          stub = pubsub_pb2_grpc.PubSubStub(channel)
          req = pubsub_pb2.SubscribeRequest(topic=topic, client_id=client_id)
          print(f"[subscriber:{client_id}] assinando '{topic}' em {target}...")
          try:
               async for msg in stub.Subscribe(req):
                    print(f"[subscriber:{client_id}] {msg.topic}@{msg.timestamp_unix_ms}: {msg.content}")
          except grpc.aio.AioRpcError as e:
               print("[subscriber] stream encerrado:", e)


if __name__ == "__main__":
     import argparse
     p = argparse.ArgumentParser()
     p.add_argument("topic")
     p.add_argument("client_id")
     p.add_argument("--target", default="localhost:50051")
     args = p.parse_args()
     asyncio.run(run(args.topic, args.client_id, args.target))