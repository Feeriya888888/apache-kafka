"""Справочник клиентов: обогащение событий сегментом (new / regular / vip).

В демо справочник вшит в код. В продакшене он был бы отдельным сервисом или
таблицей в БД, а события обогащались бы через compacted-топик с изменениями
справочника (Kafka Streams: KTable) — это записано в разделе "Развитие" README.
"""

_CUSTOMERS = [
    {"customer_id": "1", "name": "Анна",   "segment": "regular"},
    {"customer_id": "2", "name": "Борис",  "segment": "vip"},
    {"customer_id": "3", "name": "Виктор", "segment": "new"},
    {"customer_id": "4", "name": "Галина", "segment": "regular"},
    {"customer_id": "5", "name": "Дмитрий","segment": "new"},
    {"customer_id": "6", "name": "Елена",  "segment": "vip"},
    {"customer_id": "7", "name": "Жанна",  "segment": "regular"},
    {"customer_id": "8", "name": "Захар",  "segment": "new"},
]


def load_customers():
    return {c["customer_id"]: c for c in _CUSTOMERS}
