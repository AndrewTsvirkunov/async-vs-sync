# Async VS Sync
Асинхронный и синхронный парсер файлов XLS с сайта Spimex.


## Функции
- сбор ссылок на XLS файлы c сайта
- асинхронная и синхронная загрузка файлов
- парсинг excel в pandas, очистка пустых строк и столбцов
- асинхронная и синхронная вставка данных в PostgreSQL


## Установка
```
pip install -r requirements.txt
```


## Структура таблицы PostgreSQl
1. id SERIAL PRIMARY KEY
2. file TEXT
3. col1 TEXT
4. col2 TEXT
5. col3 TEXT



## Использование
В командной строке (терминале):
### Асинхронный режим
```
python async_vs_sync.py --pg postgresql://user:password@host/dbname --mode async --limit 10
```
### Синхронный режим
```
python async_vs_sync.py --pg postgresql://user:password@host/dbname --mode sync --limit 10
```
### Оба режима
```
python async_vs_sync.py --pg postgresql://user:password@host/dbname --mode both --limit 10
```
### Аргументы
- --pg — строка подключения к PostgreSQL (или используйте переменную окружения PG_URL)
- --limit — ограничение на количество файлов (по умолчанию 100)
- --mode — async, sync или both


## Сравнение скорости
| Режим | Время (сек) | Кол-во строк |
| ----- |-------------|--------------|
| Async | **9.72 с**  | 4922         |
| Sync  | 15.94 с     | 4922         |

Асинхронный вариант даёт выигрыш примерно 6 секунд при одинаковом количестве файлов (при лимите равным 100).

