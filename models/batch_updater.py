import logging
import threading
import time
from collections import namedtuple
from datetime import datetime
from queue import Queue, Empty
from typing import Optional

from sqlalchemy import text

from config.settings import settings
from models.database import SessionLocal
from models.task import TaskStatus

logger = logging.getLogger(__name__)

StatusUpdate = namedtuple(
    "StatusUpdate",
    [
        "task_id",
        "status",
        "error_message",
        "sent_at",
        "retry_count",
        "next_retry_at",
        "created_at",
    ],
)


class BatchStatusUpdater:
    def __init__(
        self,
        max_batch_size: int = None,
        flush_interval_ms: int = None,
        enabled: bool = None,
    ):
        self.max_batch_size = max_batch_size or settings.BATCH_UPDATE_MAX_SIZE
        self.flush_interval = (flush_interval_ms or settings.BATCH_UPDATE_INTERVAL_MS) / 1000.0
        self.enabled = enabled if enabled is not None else settings.BATCH_UPDATE_ENABLED
        self._queue: "Queue[StatusUpdate]" = Queue()
        self._running = False
        self._flush_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def start(self):
        if self._running:
            return
        self._running = True
        if self.enabled:
            self._flush_thread = threading.Thread(
                target=self._flush_loop, daemon=True, name="batch-updater"
            )
            self._flush_thread.start()
            logger.info(
                "Batch status updater started (batch_size=%d, interval=%.1fs)",
                self.max_batch_size, self.flush_interval,
            )
        else:
            logger.warning("Batch status updater is disabled, updates will be synchronous")

    def stop(self, flush_remaining: bool = True):
        if not self._running:
            return
        self._running = False
        if self._flush_thread:
            self._flush_thread.join(timeout=10)
        if flush_remaining and self.enabled:
            remaining = self._queue.qsize()
            if remaining > 0:
                logger.info("Flushing remaining %d status updates before shutdown", remaining)
                self._flush_batch(force=True)
        logger.info("Batch status updater stopped")

    def queue_update(
        self,
        task_id: str,
        status: TaskStatus,
        error_message: Optional[str] = None,
        sent_at: Optional[datetime] = None,
        retry_count: Optional[int] = None,
        next_retry_at: Optional[datetime] = None,
    ):
        if not self.enabled:
            self._sync_update(task_id, status, error_message, sent_at, retry_count, next_retry_at)
            return
        update = StatusUpdate(
            task_id=task_id,
            status=status,
            error_message=error_message,
            sent_at=sent_at,
            retry_count=retry_count,
            next_retry_at=next_retry_at,
            created_at=time.monotonic(),
        )
        self._queue.put(update)
        if self._queue.qsize() >= self.max_batch_size:
            with self._lock:
                if self._queue.qsize() >= self.max_batch_size:
                    self._flush_batch()

    def _sync_update(
        self,
        task_id: str,
        status: TaskStatus,
        error_message: Optional[str],
        sent_at: Optional[datetime],
        retry_count: Optional[int],
        next_retry_at: Optional[datetime],
    ):
        db = SessionLocal()
        try:
            from models import crud
            crud.update_task_status(
                db, task_id, status, error_message, sent_at, retry_count, next_retry_at
            )
        except Exception as e:
            logger.exception("Sync update failed for task %s: %s", task_id, e)
        finally:
            db.close()

    def _flush_loop(self):
        while self._running:
            try:
                time.sleep(self.flush_interval)
                if self._queue.qsize() > 0:
                    with self._lock:
                        if self._queue.qsize() > 0:
                            self._flush_batch()
            except Exception as e:
                logger.exception("Error in batch flush loop: %s", e)

    def _flush_batch(self, force: bool = False):
        if self._queue.empty():
            return

        batch = []
        try:
            while len(batch) < self.max_batch_size:
                update = self._queue.get_nowait()
                batch.append(update)
        except Empty:
            pass

        if not batch:
            return

        start_time = time.monotonic()
        try:
            self._execute_batch_update(batch)
            elapsed = (time.monotonic() - start_time) * 1000
            logger.info(
                "Batch update completed: %d rows in %.2fms (avg %.2fms/row)",
                len(batch), elapsed, elapsed / len(batch) if batch else 0,
            )
        except Exception as e:
            logger.exception(
                "Batch update failed for %d tasks, re-queueing: %s", len(batch), e
            )
            for update in batch:
                self._queue.put(update)
            time.sleep(0.1)

    def _execute_batch_update(self, batch: list[StatusUpdate]):
        db = SessionLocal()
        try:
            now = datetime.utcnow()

            when_clauses = []
            error_when_clauses = []
            sent_at_when_clauses = []
            retry_count_when_clauses = []
            next_retry_at_when_clauses = []
            task_ids = []
            params = {}

            for i, update in enumerate(batch):
                task_id_param = f"task_id_{i}"
                status_param = f"status_{i}"
                error_param = f"error_{i}"
                sent_at_param = f"sent_at_{i}"
                retry_count_param = f"retry_count_{i}"
                next_retry_at_param = f"next_retry_at_{i}"

                task_ids.append(f":{task_id_param}")
                params[task_id_param] = update.task_id
                params[status_param] = update.status.value
                params[error_param] = update.error_message
                params[sent_at_param] = update.sent_at
                params[retry_count_param] = update.retry_count
                params[next_retry_at_param] = update.next_retry_at

                when_clauses.append(f"WHEN id = :{task_id_param} THEN :{status_param}::task_status")
                error_when_clauses.append(f"WHEN id = :{task_id_param} THEN :{error_param}::text")
                sent_at_when_clauses.append(f"WHEN id = :{task_id_param} THEN :{sent_at_param}::timestamp")
                retry_count_when_clauses.append(f"WHEN id = :{task_id_param} THEN :{retry_count_param}::integer")
                next_retry_at_when_clauses.append(f"WHEN id = :{task_id_param} THEN :{next_retry_at_param}::timestamp")

            task_ids_str = ", ".join(task_ids)
            case_status = "CASE " + " ".join(when_clauses) + " END"
            case_error = "CASE " + " ".join(error_when_clauses) + " END"
            case_sent_at = "CASE " + " ".join(sent_at_when_clauses) + " END"
            case_retry_count = "CASE " + " ".join(retry_count_when_clauses) + " END"
            case_next_retry_at = "CASE " + " ".join(next_retry_at_when_clauses) + " END"

            sql = text(f"""
                UPDATE mail_tasks
                SET
                    status = {case_status},
                    error_message = COALESCE({case_error}, error_message),
                    sent_at = COALESCE({case_sent_at}, sent_at),
                    retry_count = COALESCE({case_retry_count}, retry_count),
                    next_retry_at = COALESCE({case_next_retry_at}, next_retry_at),
                    updated_at = :now
                WHERE id IN ({task_ids_str})
            """)
            params["now"] = now

            result = db.execute(sql, params)
            db.commit()

            if result.rowcount != len(batch):
                logger.warning(
                    "Batch update row count mismatch: expected %d, updated %d",
                    len(batch), result.rowcount,
                )
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()


batch_updater = BatchStatusUpdater()
