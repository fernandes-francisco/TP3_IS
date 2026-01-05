import asyncio
import socket
import requests
import json
import uuid
from datetime import datetime
RUST_HOST = "172.0.0.1"
RUST_PORT = 3003
SIZE_LIMIT = 50

async def send_batch(prices):
    try:
        reader, writer = await asyncio.open_connection(RUST_HOST, RUST_PORT)
        writer.write(prices.encode("utf-8"))
        await writer.drain()
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        print(f"Erro no serviço: {e}")

