"""Producer: генерирует события продаж и публикует их в топик sales.raw.

Данные синтетические и намеренно "грязные": дубликаты с разными timestamp,
битые суммы, неизвестные статусы, пропущенные поля. Именно такие сюрпризы
приносит реальный прод-поток.
"""
import json
from datetime import datetime, timedelta, timezone

from config import BOOTSTRAP_SERVERS, TOPIC_RAW

BASE_DT = datetime(2025, 3, 1, 10, 0, 0, tzinfo=timezone.utc)


def _iso(seconds):
    return (BASE_DT + timedelta(seconds=seconds)).isoformat()


def _e(order_id, customer_id, product, category, qty, price, status, sec):
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "product": product,
        "category": category,
        "quantity": qty,
        "price": price,
        "status": status,
        "ts": _iso(sec),
    }


RAW_EVENTS = [
    # окно 10:00:00–10:01:00
    _e("O-1001", 1, "Кофемолка", "Техника", 1, "4 990,00 руб.", "completed", 0),
    _e("O-1002", 2, "Чайник", "техника", 2, "3 150,50 руб.", "completed", 20),
    _e("O-1003", 3, "Кружка", "Дом и сад", 3, "450,00 руб.", "completed", 45),
    _e("O-1001", 1, "Кофемолка", "Техника", 1, "4 990,00 руб.", "completed", 0),     # точный дубль
    # окно 10:01:00–10:02:00
    _e("O-1004", 4, "Ноутбук", "Техника", 1, "79 900,00 руб.", "completed", 70),
    _e("O-1005", 2, "Наушники", "Техника", 1, "12 340,90 руб.", "refunded", 80),
    _e("O-1006", 5, "Скатерть", "Дом и сад", 1, "1 250,50 руб.", "completed", 95),
    _e("O-1002", 2, "Чайник", "Техника", 2, "3 150,50 руб.", "completed", 20),      # точный дубль
    # окно 10:02:00–10:03:00
    _e("O-1007", 6, "Футболка", "Одежда", 2, "1 990,00 руб.", "completed", 125),
    _e("O-1008", 7, "Кроссовки", "Одежда", 1, "5 499,00 руб.", "canceled", 150),
    _e("O-1009", 8, "Куртка", "Одежда", 1, "8 990,00 руб.", "completed", 170),
    _e("O-1010", 3, "Кофе", "Продукты", 5, "890,00 руб.", "completed", 185),
    # окно 10:03:00–10:04:00
    _e("O-1011", 1, "Кружка", "дом и сад", 1, "450,00 руб.", "completed", 230),
    _e("O-1012", 4, "Клавиатура", "Техника", 1, "6 200,00", "completed", 260),      # без "руб."
    {"order_id": "O-1013", "customer_id": "5", "product": "Сертификат", "status": "completed", "ts": _iso(280)},  # нет полей
    _e("O-1014", 6, "Гантель", "Спорт", 1, "abc", "completed", 300),                # нечитаемая сумма
    # окно 10:05:00–10:06:00
    _e("O-1015", 7, "Скакалка", "Спорт", 1, "1 200,00 руб.", "completed", 320),
    _e("O-1016", 8, "Штанга", "Спорт", 2, "0,00 руб.", "completed", 330),           # нулевая сумма
    _e("O-1017", 2, "Чашка", "Дом и сад", -1, "300,00 руб.", "completed", 340),     # отрицательное количество
    # окно 10:06:00–10:07:00
    _e("O-1018", 3, "Диван", "Мебель", 1, "45 000,00 руб.", "completed", 400),
    _e("O-1019", 4, "Стол", "Мебель", 1, "12 500,00 руб.", "completed", 430),
    _e("O-1015", 7, "Скакалка", "Спорт", 1, "1 200,00 руб.", "completed", 310),     # дубль того же события
    {"order_id": "O-1020", "product": "Ноутбук", "category": "Техника", "quantity": 1,
     "price": "99 000,00 руб.", "status": "completed", "ts": _iso(450)},            # нет customer_id
]


def create_producer():
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: None if k is None else str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=3,
    )


def run(broker=None):
    """Публикует события в TOPIC_RAW. broker=None -> настоящий Kafka."""
    if broker is None:
        from broker import KafkaBroker
        broker = KafkaBroker(BOOTSTRAP_SERVERS)
    count = 0
    for evt in RAW_EVENTS:
        broker.send(TOPIC_RAW, key=evt.get("order_id"), value=evt)
        count += 1
    # маркер конца потока: консьюмер перестаёт ждать новые сообщения
    broker.send(TOPIC_RAW, key="__eos__", value={"__eos__": True})
    broker.flush()
    return count
