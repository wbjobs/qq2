import json
import logging
import threading
import time
from datetime import datetime
from typing import Optional

import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from config.settings import settings
from consumer.rate_limiter import TokenBucket
from models import TaskStatus, batch_updater
from utils import rabbitmq_pool

logger = logging.getLogger(__name__)


class MailConsumer:
    def __init__(self):
        self.queue_name = settings.RABBITMQ_QUEUE
        self.high_priority_threshold = settings.HIGH_PRIORITY_THRESHOLD
        self.aging_threshold = settings.AGING_THRESHOLD_SECONDS
        self.aging_promotion_priority = settings.AGING_PROMOTION_PRIORITY
        self.high_priority_backlog_threshold = settings.HIGH_PRIORITY_BACKLOG_THRESHOLD
        self.rate_limiter = TokenBucket(rate=settings.NORMAL_PRIORITY_MAX_RATE)
        self._running = False
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.channel.Channel] = None
        self._last_backlog_check = 0.0
        self._current_high_priority_backlog = 0

    def _connect(self):
        credentials = pika.PlainCredentials(
            settings.RABBITMQ_USER, settings.RABBITMQ_PASSWORD
        )
        parameters = pika.ConnectionParameters(
            host=settings.RABBITMQ_HOST,
            port=settings.RABBITMQ_PORT,
            virtual_host=settings.RABBITMQ_VHOST,
            credentials=credentials,
            connection_attempts=5,
            retry_delay=2,
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            arguments={"x-max-priority": 5},
        )
        channel.basic_qos(prefetch_count=1)
        return connection, channel

    def _ensure_connection(self):
        if self._connection is None or not self._connection.is_open:
            if self._connection and self._connection.is_open:
                try:
                    self._connection.close()
                except Exception:
                    pass
            self._connection, self._channel = self._connect()
            logger.info("Consumer connected to RabbitMQ")
        if self._channel is None or not self._channel.is_open:
            self._channel = self._connection.channel()
            self._channel.queue_declare(
                queue=self.queue_name,
                durable=True,
                arguments={"x-max-priority": 5},
            )
            self._channel.basic_qos(prefetch_count=1)

    def _mock_smtp_send(
        self, task_id: str, sender: str, recipient: str, subject: str, content: str
    ) -> bool:
        logger.info(
            "[SMTP MOCK] 发送邮件 | task_id=%s | 发件人=%s | 收件人=%s | 主题=%s | 内容长度=%d",
            task_id, sender, recipient, subject, len(content),
        )
        return True

    def _check_aging_and_promote(self, message_body: dict) -> bool:
        task_id = message_body.get("task_id")
        priority = message_body.get("priority", 1)
        enqueue_at = message_body.get("enqueue_at")
        original_priority = message_body.get("original_priority", priority)

        if enqueue_at is None:
            return False

        is_high_priority = priority >= self.high_priority_threshold
        if is_high_priority:
            return False

        wait_seconds = time.time() - enqueue_at
        if wait_seconds >= self.aging_threshold:
            logger.warning(
                "Task %s aged out: waited %.1fs (threshold %ds), original priority %d, promoting to %d",
                task_id, wait_seconds, self.aging_threshold,
                original_priority, self.aging_promotion_priority,
            )
            return rabbitmq_pool.requeue_with_promotion(
                message_body, self.aging_promotion_priority
            )

        if (
            self._current_high_priority_backlog >= self.high_priority_backlog_threshold
            and wait_seconds >= self.aging_threshold / 2
        ):
            logger.warning(
                "High priority backlog detected (%d >= %d), early promotion for task %s (waited %.1fs)",
                self._current_high_priority_backlog, self.high_priority_backlog_threshold,
                task_id, wait_seconds,
            )
            return rabbitmq_pool.requeue_with_promotion(
                message_body, self.aging_promotion_priority
            )

        return False

    def _maybe_check_backlog(self):
        now = time.monotonic()
        if now - self._last_backlog_check >= 10.0:
            queue_depth, _ = rabbitmq_pool.get_queue_depth()
            self._current_high_priority_backlog = max(0, queue_depth)
            self._last_backlog_check = now
            if self._current_high_priority_backlog >= self.high_priority_backlog_threshold:
                logger.warning(
                    "High priority backlog warning: %d messages in queue (threshold %d)",
                    self._current_high_priority_backlog, self.high_priority_backlog_threshold,
                )

    def _process_message(self, message_body: dict) -> bool:
        task_id = message_body.get("task_id")
        sender = message_body.get("sender")
        recipient = message_body.get("recipient")
        subject = message_body.get("subject")
        content = message_body.get("content")
        priority = message_body.get("priority", 1)

        if not all([task_id, sender, recipient, subject, content]):
            logger.error("Invalid message format: %s", message_body)
            return False

        self._maybe_check_backlog()

        if self._check_aging_and_promote(message_body):
            logger.info("Task %s has been promoted, skipping current processing", task_id)
            return True

        batch_updater.queue_update(task_id, TaskStatus.PROCESSING)

        try:
            is_high_priority = priority >= self.high_priority_threshold
            if not is_high_priority:
                logger.debug(
                    "Normal priority task %s, waiting for rate limiter token", task_id
                )
                self.rate_limiter.acquire()
                logger.debug("Rate limiter token acquired for task %s", task_id)
            else:
                logger.info(
                    "High priority task %s (priority=%d), bypass rate limiter",
                    task_id, priority,
                )

            success = self._mock_smtp_send(
                task_id=task_id,
                sender=sender,
                recipient=recipient,
                subject=subject,
                content=content,
            )

            if success:
                batch_updater.queue_update(
                    task_id, TaskStatus.SUCCESS, sent_at=datetime.utcnow()
                )
                logger.info("Mail task %s completed successfully", task_id)
            else:
                batch_updater.queue_update(
                    task_id, TaskStatus.FAILED, error_message="SMTP send failed"
                )
                logger.error("Mail task %s failed: SMTP send returned false", task_id)

            return success
        except Exception as e:
            logger.exception("Error processing task %s: %s", task_id, e)
            batch_updater.queue_update(
                task_id, TaskStatus.FAILED, error_message=str(e)
            )
            return False

    def _on_message(self, ch, method, properties, body):
        try:
            message = json.loads(body.decode("utf-8"))
            logger.info(
                "Received message | task_id=%s | priority=%s",
                message.get("task_id"),
                properties.priority if properties else "unknown",
            )
            success = self._process_message(message)
            if success:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.exception("Failed to handle message: %s", e)
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            except Exception:
                pass

    def start(self):
        self._running = True
        logger.info("Starting mail consumer...")

        logger.info("Starting batch status updater...")
        batch_updater.start()

        while self._running:
            try:
                self._ensure_connection()
                logger.info("Waiting for messages on queue '%s'...", self.queue_name)
                self._channel.basic_consume(
                    queue=self.queue_name,
                    on_message_callback=self._on_message,
                    auto_ack=False,
                )
                self._channel.start_consuming()
            except (AMQPConnectionError, AMQPChannelError) as e:
                logger.warning("RabbitMQ connection error: %s, reconnecting in 5s...", e)
                time.sleep(5)
            except Exception as e:
                logger.exception("Consumer error: %s", e)
                if self._running:
                    time.sleep(5)

    def stop(self):
        logger.info("Stopping mail consumer...")
        self._running = False
        try:
            if self._channel and self._channel.is_open:
                self._channel.stop_consuming()
                self._channel.close()
        except Exception:
            pass
        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception:
            pass
        logger.info("Stopping batch status updater...")
        batch_updater.stop(flush_remaining=True)
        logger.info("Mail consumer stopped")


def run_consumer_in_thread() -> threading.Thread:
    consumer = MailConsumer()
    thread = threading.Thread(target=consumer.start, daemon=True, name="mail-consumer")
    thread.start()
    logger.info("Mail consumer thread started")
    return thread
