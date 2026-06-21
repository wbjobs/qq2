import logging
import threading
import time
from datetime import datetime
from typing import Optional

from config.settings import settings
from models import SessionLocal, crud
from utils import rabbitmq_pool

logger = logging.getLogger(__name__)


class TaskScheduler:
    def __init__(self):
        self.poll_interval = settings.SCHEDULER_POLL_INTERVAL_SECONDS
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._scheduled_count = 0
        self._retry_count = 0

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop, daemon=True, name="task-scheduler"
        )
        self._thread.start()
        logger.info(
            "Task scheduler started (poll_interval=%ds)", self.poll_interval
        )

    def stop(self):
        if not self._running:
            return
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        logger.info(
            "Task scheduler stopped (scheduled=%d, retried=%d)",
            self._scheduled_count, self._retry_count,
        )

    def _poll_loop(self):
        while self._running:
            try:
                self._poll_scheduled_tasks()
                self._poll_retry_tasks()
            except Exception as e:
                logger.exception("Scheduler poll error: %s", e)

            sleep_until = time.monotonic() + self.poll_interval
            while self._running and time.monotonic() < sleep_until:
                time.sleep(min(1.0, sleep_until - time.monotonic()))

    def _poll_scheduled_tasks(self):
        db = SessionLocal()
        try:
            tasks = crud.fetch_due_scheduled_tasks(db, limit=100)
            if not tasks:
                return

            logger.info("Found %d due scheduled tasks", len(tasks))
            enqueued = 0
            for task in tasks:
                try:
                    published = rabbitmq_pool.publish_message(
                        task_id=task.id,
                        sender=task.sender,
                        recipient=task.recipient,
                        subject=task.subject,
                        content=task.content,
                        priority=task.priority,
                    )
                    if published:
                        crud.mark_task_enqueued(db, task.id)
                        enqueued += 1
                        self._scheduled_count += 1
                    else:
                        logger.warning(
                            "Failed to enqueue scheduled task %s, will retry next poll",
                            task.id,
                        )
                except Exception as e:
                    logger.exception(
                        "Error enqueuing scheduled task %s: %s", task.id, e
                    )
            if enqueued > 0:
                logger.info("Enqueued %d scheduled tasks", enqueued)
        finally:
            db.close()

    def _poll_retry_tasks(self):
        db = SessionLocal()
        try:
            tasks = crud.fetch_due_retry_tasks(db, limit=100)
            if not tasks:
                return

            logger.info("Found %d due retry tasks", len(tasks))
            enqueued = 0
            for task in tasks:
                try:
                    published = rabbitmq_pool.publish_message(
                        task_id=task.id,
                        sender=task.sender,
                        recipient=task.recipient,
                        subject=task.subject,
                        content=task.content,
                        priority=task.priority,
                    )
                    if published:
                        crud.mark_task_enqueued(db, task.id)
                        enqueued += 1
                        self._retry_count += 1
                    else:
                        logger.warning(
                            "Failed to enqueue retry task %s, will retry next poll",
                            task.id,
                        )
                except Exception as e:
                    logger.exception(
                        "Error enqueuing retry task %s: %s", task.id, e
                    )
            if enqueued > 0:
                logger.info("Enqueued %d retry tasks", enqueued)
        finally:
            db.close()


task_scheduler = TaskScheduler()
