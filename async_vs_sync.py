import asyncio
import aiohttp
import io
import time
import argparse
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

URL = "https://spimex.com/markets/oil_products/trades/results/"


async def fetch_links(limit=None):
    """
    Асинхронно собирает ссылки на XLS файлы с сайта SPIMEX.
    :param limit: (int, optional): максимальное кол-во ссылок, если None, то все
    :return: list: список ссылок на XLS файлы
    """
    async with aiohttp.ClientSession() as session:
        request = await session.get(URL)
        html = await request.text()
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a["href"]
        if "xls" in href.lower():
            links.append(href if href.startswith("http") else urljoin(URL, href))
    return links[:limit] if limit else links


async def download(url):
    """
    Асинхронно скачивает XLS файлы по URL.
    :param url: (str): ссылка на файл
    :return: tuple: (имя файла, бинарные данные файла)
    """
    async with aiohttp.ClientSession() as session:
        request = await session.get(url)
        return url.rsplit("/", 1)[-1], await request.read()


def parse_xls(name, data):
    """
    Парсит XLS файл в pandas DataFrame, удаляет пустые строки и столбцы,
    возвращает список кортежей для вставки в базу данных.
    :param name: (str): имя файла
    :param data: (bytes): содержимое XLS файла
    :return: list: список кортежей (file, col1, col2, col3)
    """
    df = pd.read_excel(io.BytesIO(data), engine="xlrd", header=None)
    df.dropna(axis=0, how='all', inplace=True)
    df.dropna(axis=1, how='all', inplace=True)
    rows = df.iloc[30:, :3].astype(str).fillna("").values.tolist()
    return [(name, *row) for row in rows]


async def async_pipeline(pg_url, limit):
    """
    Асинхронная загрузка XLS файлов, парсинг и вставка в PostgreSQL.
    :param pg_url: (str): URL подключения к БД
    :param limit: (int): лимит на кол-во файлов
    :return: (время выполнения, кол-во строк)
    """
    async_url = pg_url.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(async_url, echo=False)

    # создаем таблицу (если нет)
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS trades(
              id SERIAL PRIMARY KEY,
              file TEXT, col1 TEXT, col2 TEXT, col3 TEXT)
        """))

    t0 = time.perf_counter()
    links = await fetch_links(limit)
    files = await asyncio.gather(*[download(u) for u in links])
    rows = []
    for name, data in files:
        rows.extend(parse_xls(name, data))

    async with engine.begin() as conn:
        stmt = text("INSERT INTO trades(file, col1, col2, col3) VALUES (:file,:col1,:col2,:col3)")
        for f, c1, c2, c3 in rows:
            await conn.execute(stmt, {"file": f, "col1": c1, "col2": c2, "col3": c3})
    await engine.dispose()
    return time.perf_counter() - t0, len(rows)


def sync_pipeline(pg_url, limit):
    """
    Синхронная загрузка XLS файлов, парсинг и вставка в PostgreSQL.
    :param pg_url: (str): URL подключения к БД
    :param limit: (int): лимит на кол-во файлов
    :return: tuple: (время выполнения, кол-во строк)
    """
    sync_url = pg_url.replace("postgresql://", "postgresql+psycopg2://")
    engine = create_engine(sync_url, echo=False)

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS trades(
              id SERIAL PRIMARY KEY,
              file TEXT, col1 TEXT, col2 TEXT, col3 TEXT)
        """))

    t0 = time.perf_counter()
    request = requests.get(URL)
    soup = BeautifulSoup(request.text, "lxml")
    links = [urljoin(URL, a["href"]) for a in soup.select("a[href]") if "xls" in  a["href"].lower()]
    links = links[:limit]

    rows = []
    for u in links:
        rr = requests.get(u)
        name = u.rsplit("/", 1)[-1]
        df = pd.read_excel(io.BytesIO(rr.content), engine="xlrd", header=None)
        df.dropna(axis=0, how='all', inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
        rows.extend([(name, *row) for row in df.iloc[30:, :3].astype(str).fillna("").values.tolist()])

    with engine.begin() as conn:
        stmt = text("INSERT INTO trades(file, col1, col2, col3) VALUES (:file,:col1,:col2,:col3)")
        for f, c1, c2, c3 in rows:
            conn.execute(stmt, {"file": f, "col1": c1, "col2": c2, "col3": c3})
    return time.perf_counter() - t0, len(rows)


async def main():
    """ CLI. Точка входа. Поддерживает режимы async, sync или оба. """
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg", default=os.environ.get("PG_URL"))
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--mode", choices=["async", "sync", "both"], default="both")
    args = ap.parse_args()

    if not args.pg:
        raise SystemExit("Укажите --pg или переменную окружения PG_URL")

    if args.mode in ["async", "both"]:
        t, n = await async_pipeline(args.pg, args.limit)
        print(f"ASYNC: {n} строк за {t:.2f} с")
    if args.mode in ["sync", "both"]:
        t, n = sync_pipeline(args.pg, args.limit)
        print(f"SYNC: {n} строк за {t:.2f} с")


if __name__ == "__main__":
    asyncio.run(main())
