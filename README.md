# redis-route-worker

Небольшой Python-пакет для обработки задач через Redis Lists и Pub/Sub.
Несколько очередей работают в одном процессе: у каждой свой обработчик,
событие завершения и пул потоков. Формат содержимого задания выбирает приложение.

Требуются Python 3.12+ и доступный Redis. Единственная runtime-зависимость — `redis`.

## Установка

Из каталога пакета или клонированного репозитория:

```bash
python -m pip install .
# Для разработки:
python -m pip install -e ".[dev]"
```

После размещения на GitHub (замените OWNER и REPOSITORY):

```bash
python -m pip install "redis-route-worker @ git+https://github.com/vmrfriz/redis-route-worker.git@v0.1.0"
```

Зависимость другого проекта в `pyproject.toml`:

```toml
[project]
dependencies = [
    "redis-route-worker @ git+https://github.com/vmrfriz/redis-route-worker.git@v0.1.0",
]
```

Установка по имени `pip install redis-route-worker` предполагает отдельную
публикацию в PyPI; размещение репозитория в GitHub этого не делает.

## Быстрый старт

Сохраните как `worker.py`:

```python
import json

from redis_route_worker import QueueRoute, RedisWorker, TaskContext


def create_preview(raw: str, context: TaskContext) -> None:
    task = json.loads(raw)
    # Выполнить работу приложения.
    context.complete(json.dumps({"id": task["id"], "status": "ready"}))


def export(raw: str, context: TaskContext) -> None:
    context.complete(raw)


if __name__ == "__main__":
    RedisWorker(
        routes=[
            QueueRoute("preview", "newPreview", "previewReady", create_preview, workers=2),
            QueueRoute("export", "newExport", "exportReady", export, workers=1),
        ],
        prefix="myapp",
        host="localhost",
        port=6379,
    ).run()
```

Запустите `python worker.py`. Отправитель задания:

```python
import json
import redis

client = redis.Redis(host="localhost", port=6379, decode_responses=True)
with client.pipeline(transaction=True) as transaction:
    transaction.rpush("myapp_previewList", json.dumps({"id": 42}))
    transaction.publish("myapp_preview", "newPreview")
    transaction.execute()
client.close()
```

Результат будет в `myapp_previewCompleteList`; в канал `myapp_preview`
поступит `previewReady`. Получить результат можно через `LPOP` или `BLPOP`.
Уведомления Pub/Sub не хранят результаты: источником результата является список.

Готовый пример с двумя маршрутами:
`python examples/two_routes.py /tmp/storage`.

## Поведение

- При запуске выполняются задания, уже находящиеся в списках.
- Входное событие будит потоки своего маршрута. Дополнительная проверка раз
  в секунду позволяет обрабатывать задания при потере уведомления.
- Пулы маршрутов независимы; суммарный лимит равен сумме `workers`.
- Обработчик явно вызывает `context.complete(data)`. Возврат из функции ничего
  не публикует; несколько вызовов `complete()` создают несколько результатов.
- Необработанное исключение логируется, исходная строка возвращается в конец
  исходной очереди, публикуется входное событие. Повторы немедленные и не ограничены.
- SIGINT/SIGTERM прекращают получение новых заданий и дожидаются активных
  обработчиков. Программно остановка запрашивается через `worker.stop()`.

## Ограничения

Используется извлечение через `LPOP`: аварийное завершение процесса после
извлечения может потерять задание. Подтверждений, персистентного списка активных
заданий и гарантии exactly-once нет. Делайте операции идемпотентными, если возможны
повторы. Публикация результата не объединяется в транзакцию с действиями приложения.

Постоянно падающее задание повторяется без задержки и может создавать нагрузку.
Ошибки соединения с Redis завершают воркер; автоматического восстановления
соединения на уровне пакета нет. Для перезапуска используйте менеджер процессов.

Обработчики синхронные; `async def` не поддерживается. При `workers > 1` обработчик
и его общие объекты должны быть потокобезопасными; порядок завершения не гарантирован.
Остановка не прерывает зависший обработчик. Сигналы устанавливаются только при
запуске `run()` в главном потоке; при запуске в другом потоке вызывайте `stop()`.

## Документация

- [API](docs/api.md): маршруты, контекст и жизненный цикл.
- [Протокол Redis](docs/protocol.md): ключи, события и интеграция с другими языками.
