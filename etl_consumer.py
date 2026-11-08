"""Стрим-процессор: читает sales.raw, чистит поток и строит витрину.

Шаги конвейера:
1) валидация и нормализация события (transform.clean_event);
2) дедупликация по order_id — храним timestamp последней увиденной версии;
3) обогащение сегментом клиента из справочника;
4) агрегация в tumbling-окна (AGG_WINDOW_SEC) с допуском на опоздания;
5) публикация: очищенные записи -> sales.clean, витрина -> sales.mart,
   непрошедшие валидацию -> sales.dead.
"""
from collections import defaultdict
from datetime import timedelta

from config import AGG_WINDOW_SEC, LATE_TOLERANCE_SEC, TOPIC_CLEAN, TOPIC_DLQ, TOPIC_MART, TOPIC_RAW
from transform import clean_event, window_start


class Pipeline:
    """Состояние стрим-процессора. process() вызывается на каждое сообщение."""

    def __init__(self, customers):
        self.customers = customers          # customer_id -> {"segment": ...}
        self.seen = {}                      # order_id -> ts последней версии
        self.windows = defaultdict(lambda: {"revenue": 0.0, "orders": 0, "qty": 0})
        self.max_ts = None                  # watermark: максимум ts среди увиденных событий
        self.ready = []                     # закрытые окна витрины
        self.stats = {"raw": 0, "cleaned": 0, "duplicate": 0, "dlq": 0, "windows": 0}

    def process(self, raw):
        self.stats["raw"] += 1

        rec, reason = clean_event(raw)
        if rec is None:
            self.stats["dlq"] += 1
            return {"status": "dlq", "reason": reason}

        oid, ts = rec["order_id"], rec["ts"]
        prev_ts = self.seen.get(oid)
        if prev_ts is not None and prev_ts >= ts:
            self.stats["duplicate"] += 1
            return {"status": "duplicate"}

        self.seen[oid] = ts
        rec["segment"] = self.customers.get(rec["customer_id"], {}).get("segment", "unknown")
        self.stats["cleaned"] += 1
        self._accumulate(rec)
        return {"status": "ok", "record": rec}

    def _accumulate(self, rec):
        if self.max_ts is None or rec["ts"] > self.max_ts:
            self.max_ts = rec["ts"]
        key = (rec["category"], window_start(rec["ts"], AGG_WINDOW_SEC))
        cell = self.windows[key]
        cell["orders"] += 1
        cell["qty"] += rec["quantity"]
        if rec["status"] == "completed":   # refunded/canceled учитываются в заказах, но не в выручке
            cell["revenue"] += rec["amount"]
        self._flush_ready()

    def _flush_ready(self):
        """Закрывает окна, которые уже не могут получить новые события."""
        horizon = self.max_ts - timedelta(seconds=LATE_TOLERANCE_SEC)
        for key in list(self.windows):
            if key[1] + timedelta(seconds=AGG_WINDOW_SEC) <= horizon:
                self._emit(key)

    def _emit(self, key):
        category, w = key
        cell = self.windows.pop(key)
        self.ready.append({
            "category": category,
            "window_start": w.isoformat(),
            "revenue": round(cell["revenue"], 2),
            "orders": cell["orders"],
            "quantity": cell["qty"],
        })
        self.stats["windows"] += 1

    def finish(self):
        """Конец потока: сбрасываем все оставшиеся окна."""
        while self.windows:
            self._emit(next(iter(self.windows)))
        return self.ready


def run(broker, customers, limit=10000):
    """Читает TOPIC_RAW до маркера конца потока, публикует результаты."""
    pipeline = Pipeline(customers)
    processed = 0
    for key, value in broker.consume(TOPIC_RAW, group_id="etl-processor"):
        if isinstance(value, dict) and value.get("__eos__"):
            break
        result = pipeline.process(value)
        if result["status"] == "dlq":
            broker.send(TOPIC_DLQ, key=value.get("order_id"),
                        value={"raw": value, "reason": result["reason"]})
        elif result["status"] == "ok":
            broker.send(TOPIC_CLEAN, key=result["record"]["order_id"], value=_to_jsonable(result["record"]))
        processed += 1
        if processed >= limit:
            break

    rows = pipeline.finish()
    for row in rows:
        broker.send(TOPIC_MART, key=row["category"], value=row)
    broker.send(TOPIC_MART, key="__eos__", value={"__eos__": True})
    broker.flush()
    return {"rows": rows, "stats": pipeline.stats}


def _to_jsonable(rec):
    out = dict(rec)
    out["ts"] = rec["ts"].isoformat()
    return out
