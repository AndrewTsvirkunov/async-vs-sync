import os
import sys
import re
import ssl
import asyncio
import aiohttp
import aiofiles
import argparse
import urllib.request
from urllib.parse import urlsplit
import time
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, Integer, Float, String, Date, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker as async_sessionmaker


URL = "https://spimex.com/markets/oil_products/trades/results/"

Base = declarative_base()


class SpimexTradingResults(Base):
    __tablename__ = "spimex_trading_results"

    id = Column(Integer, primary_key=True)
    exchange_product_id = Column(String)
    exchange_product_name = Column(String)
    oil_id = Column(String)
    delivery_basis_id = Column(String)
    delivery_basis_name = Column(String)
    delivery_type_id = Column(String)
    volume = Column(Float)
    total = Column(Float)
    count = Column(Integer)
    date = Column(Date)
    created_on = Column(DateTime)
    updated_on = Column(DateTime)


def init_db_sync(pg_url):
    """
    Инициализирует синхронное подключение к PostgreSQL и создаёт таблицы, если они не существуют.
    Args:
        pg_url (str): URL подключения к базе данных.
    Returns:
        Engine: SQLAlchemy Engine для синхронной работы с PostgreSQL.
    """
    sync_url = pg_url.replace("postgresql://", "postgresql+psycopg2://")
    engine = create_engine(sync_url)
    Base.metadata.create_all(engine)
    return engine


async def init_db_async(pg_url):
    """
    Асинхронно инициализирует подключение к PostgreSQL и создаёт таблицы, если они не существуют.
    Args:
        pg_url (str): URL подключения к базе данных.
    Returns:
        AsyncEngine: SQLAlchemy AsyncEngine для асинхронной работы с PostgreSQL.
    """
    async_url = pg_url.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(async_url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine


def get_bulletin_url():
    """
    Собирает ссылки на XLS-файлы с результатами торгов нефтью за последние годы (2023–2025).
    Returns:
        list[str]: Список URL XLS-файлов.
    """
    response = requests.get(URL)
    soup = BeautifulSoup(response.content, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".xls" in href and any(year in href for year in ["2023", "2024", "2025"]):
            links.append("http://spimex.com" + href)
    return links


def download_excel(url, folder="bulletin_sync"):
    """
    Скачивает XLS-файл с указанного URL в локальную папку синхронно.
    Args:
        url (str): Ссылка на XLS-файл.
        folder (str): Папка для сохранения файла (по умолчанию "bulletin_sync").
    Returns:
        str: Полный путь к сохранённому файлу.
    """
    os.makedirs(folder, exist_ok=True)
    parsed_url = urlsplit(url)
    filename = os.path.basename(parsed_url.path)
    filename_path = os.path.join(folder, filename)
    if not os.path.exists(filename_path):
        ssl._create_default_https_context = ssl._create_unverified_context
        urllib.request.urlretrieve(url, filename_path)
    return filename_path


async def download_excel_async(url, folder="bulletin_async"):
    """
    Асинхронно скачивает XLS-файл с указанного URL в локальную папку.
    Args:
        url (str): Ссылка на XLS-файл.
        folder (str): Папка для сохранения файла (по умолчанию "bulletin_async").
    Returns:
        str: Полный путь к сохранённому файлу.
    """
    os.makedirs(folder, exist_ok=True)
    parsed_url = urlsplit(url)
    filename = os.path.basename(parsed_url.path)
    filename_path = os.path.join(folder, filename)

    if not os.path.exists(filename_path):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                f = await aiofiles.open(filename_path, mode="wb")
                await f.write(await resp.read())
                await f.close()
    return filename_path


def extract_date_from_filename(filename):
    """
    Извлекает дату из имени XLS-файла в формате YYYYMMDD.
    Args:
        filename (str): Имя файла.
    Returns:
        datetime.date | None: Дата из имени файла или None, если не удалось распарсить.
    """
    match = re.search(r"(\d{4})(\d{2})(\d{2})\d{6}", filename)
    if match:
        return datetime.strptime(match.group(0)[:8], "%Y%m%d").date()
    return None


def parse_trading_section(file_path):
    """
    Парсит таблицу торгов из XLS-файла, фильтрует строки с количеством договоров > 0
    и добавляет вычисляемые колонки.
    Args:
        file_path (str): Путь к XLS-файлу.
    Returns:
        pandas.DataFrame: DataFrame.
    """
    xls = pd.ExcelFile(file_path, engine="xlrd")
    for sheet in xls.sheet_names:
        df = pd.read_excel(file_path, sheet_name=sheet, engine="xlrd", header=None)
        start_idx = df[df[1].str.contains("Единица измерения: Метрическая тонна", na=False)].index[0]

        next_sections = df[df[1].str.startswith("Единица измерения:", na=False)].index
        next_section_idx = next_sections[next_sections > start_idx][0] if any(next_sections > start_idx) else len(df)

        data = df.iloc[start_idx:next_section_idx]
        headers = data.iloc[1].tolist()
        data = data.iloc[2:].reset_index(drop=True)
        data.columns = headers

        data = data[
            data["Код\nИнструмента"].notna() &
            ~data["Код\nИнструмента"].astype(str).str.contains("Итого:", na=False)
        ]

        data["Количество\nДоговоров,\nшт."] = pd.to_numeric(data["Количество\nДоговоров,\nшт."], errors="coerce")
        data = data[data["Количество\nДоговоров,\nшт."] > 0]

        result = data[[
            "Код\nИнструмента",
            "Наименование\nИнструмента",
            "Базис\nпоставки",
            "Объем\nДоговоров\nв единицах\nизмерения",
            "Обьем\nДоговоров,\nруб.",
            "Количество\nДоговоров,\nшт."
        ]].rename(columns={
            "Код\nИнструмента": "exchange_product_id",
            "Наименование\nИнструмента": "exchange_product_name",
            "Базис\nпоставки": "delivery_basis_name",
            "Объем\nДоговоров\nв единицах\nизмерения": "volume",
            "Обьем\nДоговоров,\nруб.": "total",
            "Количество\nДоговоров,\nшт.": "count"
        })

        result["volume"] = pd.to_numeric(result["volume"], errors="coerce")
        result["total"] = pd.to_numeric(result["total"], errors="coerce")
        result["count"] = pd.to_numeric(result["count"], errors="coerce")
        result = result.dropna()

        result["oil_id"] = result["exchange_product_id"].str[:4]
        result["delivery_basis_id"] = result["exchange_product_id"].str[4:7]
        result["delivery_type_id"] = result["exchange_product_id"].str[-1]

        result["date"] = extract_date_from_filename(file_path)
        result["created_on"] = datetime.now()
        result["updated_on"] = datetime.now()

        return result


def insert_to_db_sync(engine, df):
    """
    Вставляет данные DataFrame в таблицу `spimex_trading_results` синхронно
    с обработкой ошибок и откатом транзакции при сбое.
    Args:
        engine (Engine): SQLAlchemy Engine для подключения к базе данных.
        df (pandas.DataFrame): DataFrame с данными для вставки.
    Returns:
        None
    """
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        for _, row in df.iterrows():
            record = SpimexTradingResults(**row.to_dict())
            session.add(record)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"DB error: {e}")
    finally:
        session.close()


async def insert_to_db_async(engine, df):
    """
    Асинхронно вставляет данные DataFrame в таблицу `spimex_trading_results`
    с обработкой ошибок и откатом транзакции при сбое.
    Args:
        engine (AsyncEngine): SQLAlchemy AsyncEngine для подключения к базе данных.
        df (pandas.DataFrame): DataFrame с данными для вставки.
    Returns:
        None
    """
    Session = async_sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)
    async with Session() as session:
        try:
            async with session.begin():
                objs = [SpimexTradingResults(**row.to_dict()) for _, row in df.iterrows()]
                session.add_all(objs)
        except Exception as e:
            await session.rollback()
            print(f"DB error: {e}")


# --- PIPELINES ---
def sync_pipeline(pg_url, limit):
    """
    Синхронно скачивает XLS-файлы, парсит их и вставляет данные в PostgreSQL.
    Args:
        pg_url (str): URL подключения к базе данных.
        limit (int): Максимальное количество файлов для обработки.
    Returns:
        tuple[float, int]: Время выполнения в секундах и количество обработанных строк.
    """
    engine = init_db_sync(pg_url)
    links = get_bulletin_url()[:limit]
    t0 = time.perf_counter()
    rows = 0
    for url in links:
        file_path = download_excel(url)
        df = parse_trading_section(file_path)
        insert_to_db_sync(engine, df)
        rows += len(df)
    return time.perf_counter() - t0, rows


async def async_pipeline(pg_url, limit):
    """
    Асинхронно скачивает XLS-файлы, парсит их и вставляет данные в PostgreSQL.
    Args:
        pg_url (str): URL подключения к базе данных.
        limit (int): Максимальное количество файлов для обработки.
    Returns:
       tuple[float, int]: Время выполнения в секундах и количество обработанных строк.
    """
    engine = await init_db_async(pg_url)
    links = get_bulletin_url()[:limit]
    t0 = time.perf_counter()
    files = await asyncio.gather(*[download_excel_async(u) for u in links])
    dfs = [parse_trading_section(f) for f in files]
    rows = sum(len(df) for df in dfs)
    await asyncio.gather(*[insert_to_db_async(engine, df) for df in dfs])
    return time.perf_counter() - t0, rows


# --- CLI ---
async def main():
    """
    CLI-обработчик. Позволяет выбирать режим работы (sync, async, both),
    задавать лимит файлов и URL базы данных PostgreSQL.
    Аргументы командной строки:
        --mode {async, sync, both}: Режим работы.
        --limit LIMIT: Максимальное количество XLS-файлов для обработки.
        --pg PG: URL подключения к базе данных PostgreSQL.
    Returns:
        None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["async", "sync", "both"], default="both")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--pg", type=str, default="postgresql://postgres:admin@localhost:5432/spimex")
    args = parser.parse_args()

    if args.mode in ["sync", "both"]:
        t, n = sync_pipeline(args.pg, args.limit)
        print(f"SYNC: {n} строк за {t:.2f} c")

    if args.mode in ["async", "both"]:
        t, n = await async_pipeline(args.pg, args.limit)
        print(f"ASYNC: {n} строк за {t:.2f} c")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
