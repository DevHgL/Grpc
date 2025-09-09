import asyncio
import time
from concurrent import futures
import grpc

from generated import pubsub_pb2, pubsub_pb2_grpc


class PubSubService(pubsub_pb2_grpc.PubSubServicer):
    def __init__(self):
        # tópico -> set de filas asyncio.Queue
        self.subscribers = {}  # dict[str, set[asyncio.Queue]]
        self.lock = asyncio.Lock()

    async def _register_subscriber(self, topic: str, queue: asyncio.Queue):
        async with self.lock:
            if topic not in self.subscribers:
                self.subscribers[topic] = set()
            self.subscribers[topic].add(queue)

    async def _unregister_subscriber(self, topic: str, queue: asyncio.Queue):
        async with self.lock:
            if topic in self.subscribers and queue in self.subscribers[topic]:
                self.subscribers[topic].remove(queue)
                if not self.subscribers[topic]:
                    del self.subscribers[topic]

    async def _broadcast(self, topic: str, content: str):
        now_ms = int(time.time() * 1000)
        async with self.lock:
            queues = list(self.subscribers.get(topic, []))
        for q in queues:
            await q.put(
                pubsub_pb2.Message(
                    topic=topic,
                    content=content,
                    timestamp_unix_ms=now_ms
                )
            )

    # gRPC: server-streaming
    async def Subscribe(self, request, context):
        topic = request.topic
        queue = asyncio.Queue()
        await self._register_subscriber(topic, queue)
        print(f"[broker] {request.client_id or 'anon'} assinou '{topic}'")
        try:
            while True:
                msg = await queue.get()
                yield msg
        except asyncio.CancelledError:
            pass
        finally:
            await self._unregister_subscriber(topic, queue)
            print(f"[broker] subscriber desconectou de '{topic}'")

    # gRPC: unary-unary
    async def Publish(self, request, context):
        await self._broadcast(request.topic, request.content)
        info = f"Mensagem publicada em '{request.topic}'"
        print(
            f"[broker] {request.client_id or 'anon'} publicou em "
            f"'{request.topic}': {request.content}"
        )
        return pubsub_pb2.PublishReply(ok=True, info=info)


async def serve(host: str = "0.0.0.0", port: int = 50051):
    # executor é opcional; pode usar só grpc.aio.server() também
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
    pubsub_pb2_grpc.add_PubSubServicer_to_server(PubSubService(), server)
    server.add_insecure_port(f"{host}:{port}")
    print(f"[broker] gRPC rodando em {host}:{port}")
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
