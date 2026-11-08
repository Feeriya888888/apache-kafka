"""Абстракция брокера.

ETL-логика зависит только от интерфейса send() / consume(), поэтому один и тот же
код работает и с настоящим Kafka (KafkaBroker), и с in-memory эмуляцией
(FakeBroker) — её удобно использовать в тестах и для локальной демонстрации
без Docker.
"""
import json
import time
from collections import defaultdict, deque


class KafkaBroker:
    """Обёртка над kafka-python: продюсер и консьюмеры с десериализацией JSON."""

    def __init__(self, bootstrap_servers):
        self.bootstrap = bootstrap_servers
        self._producer = None
        self._consumers = {}

    def _get_producer(self):
        if self._producer is None:
            from kafka import KafkaProducer
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap,
                key_serializer=lambda k: None if k is None else str(k).encode("utf-8"),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=5,
            )
        return self._producer

    def send(self, topic, key=None, value=None):
        self._get_producer().send(topic, key=key, value=value)

    def flush(self):
        if self._producer is not None:
            self._producer.flush()

    def consume(self, topic, group_id="etl-consumer"):
        if topic not in self._consumers:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap,
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                key_deserializer=lambda b: b.decode("utf-8") if b else None,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
            # ждём, пока топик создастся и партиции назначатся этой группе
            for _ in range(60):
                if consumer.partitions_for_topic(topic):
                    consumer.poll(timeout_ms=1000)
                    break
                time.sleep(0.5)
            self._consumers[topic] = consumer
        return self._consumers[topic]

    def close(self):
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
        for consumer in self._consumers.values():
            consumer.close()


class FakeBroker:
    """In-memory эмуляция брокера для тестов и запуска без Docker/Kafka."""

    def __init__(self):
        self._store = defaultdict(list)

    def send(self, topic, key=None, value=None):
        self._store[topic].append((key, value))

    def flush(self):
        pass

    def consume(self, topic, group_id=None):
        return iter(list(self._store[topic]))

    def close(self):
        pass
