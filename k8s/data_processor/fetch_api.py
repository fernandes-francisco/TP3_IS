import yfinance as yf
import pandas as pd
from send import send_batch
from helper import dicts_to_csv
import csv
import os
import asyncio
import numpy as np

#config
COBOL_HOST = "172.0.0.1"
COBOL_PORT = 4000
SIZE_LIMIT = 50


#get prices per ticker
async def get_prices(filename: str):
    sector_cache = {}

    with open(filename, newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames.copy()

        for i in range(10):
            col = f"p{i+1}"
            if col not in header:
                header.append(col)

        for col in ("avg_10", "sector", "volume_10"):
            if col not in header:
                header.append(col)

        buffer = []

        for row in reader:
            buffer.append(row)

            if len(buffer) < SIZE_LIMIT:
                continue

            await process_batch(buffer, header, sector_cache)
            buffer.clear()   

        if buffer:
            await process_batch(buffer, header, sector_cache)
            buffer.clear()

async def process_batch(rows, header, sector_cache):
    tickers = [row["Ticker"] for row in rows]

    prices = yf.download(
        tickers,
        period="1mo",
        group_by="ticker",
        threads=True
    )

    for row in rows:
        ticker = row["Ticker"]

        df = prices[ticker] if isinstance(prices.columns, pd.MultiIndex) else prices

        closes = df["Close"].dropna().tail(10).tolist()
        volumes = df["Volume"].dropna().tail(10).tolist()

        if len(closes) < 10 or len(volumes) < 10:
            continue

        for i, v in enumerate(closes):
            row[f"p{i+1}"] = v

        row["avg_10"] = await media_10(closes)
        row["volume_10"] = np.mean(volumes)

        if ticker not in sector_cache:
            sector_cache[ticker] = yf.Ticker(ticker).info.get("sector", "Unknown")

        row["sector"] = sector_cache[ticker]

    await send_batch(dicts_to_csv(rows, header))


def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


#calculate average using a COBOL service
async def media_10(prices: list[float]):
    try:
        payload = ";".join(f"{p:.2f}" for p in prices) + "\n"

        reader, writer = await asyncio.open_connection(
            COBOL_HOST, COBOL_PORT
        )

        writer.write(payload.encode())
        await writer.drain()

        data = await reader.read(1024)

        writer.close()
        await writer.wait_closed()

        return data.decode().strip()

    except Exception as e:
        print(f"Erro ao falar com o serviço: {e}")
        return "N/A"    
