import json
import logging
import time
from queue import Queue, Empty
from typing import Optional

import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from config.settings import settings

logger = logging.getLogger(__name__)


class RabbitMQPool:
    def __init__(
        self,
        host: str = None,
        port: int = None,
        username: str = None,
        password: str = None,
        vhost: str = None,
        pool_size: int = None,
        queue_name: str = None,
    ):
        self.host = host or settings.RABBITMQ_HOST
        self.port = port or settings.RABBITMQ_PORT
        self.username = username or settings.RABBITMQ_USER
        self.password = password or settings.RABBITMQ_PASSWORD
        self.vhost = vhost or settings.RABBITMQ_VHOST
        self.pool_size = pool_size or settings.RABBITMQ_POOL_SIZE
        self.queue_name = queue_name or settings.RABBITMQ_QUEUE
        self._pool: Queue = Queue(maxsize=self.pool_size)
        self._initialized = False

    def _create_connection(self):
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.vhost,
            credentials=credentials,
            connection_attempts=3,
            retry_delay=1,
        )
        return pika.BlockingConnection(parameters)

    def _create_channel(self, connection):
        channel = connection.channel()
        channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            arguments={"x-max-priority": 5},
        )
        return channel

    def initialize(self):
        if self._initialized:
            return
        for _ in range(self.pool_size):
            conn = self._create_connection()
            ch = self._create_channel(conn)
            self._pool.put((conn, ch))
        self._initialized = True
        logger.info("RabbitMQ connection pool initialized with size %d", self.pool_size)

    def acquire(self, timeout: float = 5.0):
        try:
            conn, ch = self._pool.get(timeout=timeout)
            if not conn.is_open or not ch.is_open:
                try:
                    if conn.is_open:
                        conn.close()
                except Exception:
                    pass
                conn = self._create_connection()
                ch = self._create_channel(conn)
            return conn, ch
        except Empty:
            raise RuntimeError("RabbitMQ connection pool exhausted")

    def release(self, conn, ch):
        try:
            if conn.is_open and ch.is_open:
                self._pool.put((conn, ch))
            else:
                try:
                    if conn.is_open:
                        conn.close()
                except Exception:
                    pass
                conn = self._create_connection()
                ch = self._create_channel(conn)
                self._pool.put((conn, ch))
        except Exception as e:
            logger.warning("Failed to release RabbitMQ connection: %s", e)

    def publish_message(
        self,
        task_id: str,
        sender: str,
        recipient: str,
        subject: str,
        content: str,
        priority: int,
        original_priority: int = None,
        enqueue_at: float = None,
    ) -> bool:
        conn = None
        ch = None
        try:
            conn, ch = self.acquire()
            message = {
                "task_id": task_id,
                "sender": sender,
                "recipient": recipient,
                "subject": subject,
                "content": content,
                "priority": priority,
                "original_priority": original_priority if original_priority is not None else priority,
                "enqueue_at": enqueue_at if enqueue_at is not None else time.time(),
            }
            properties = pika.BasicProperties(
                delivery_mode=2,
                priority=priority,
            )
            ch.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=json.dumps(message),
                properties=properties,
            )
            logger.info("Published mail task %s with priority %d", task_id, priority)
            return True
        except (AMQPConnectionError, AMQPChannelError) as e:
            logger.error("Failed to publish message: %s", e)
            return False
        finally:
            if conn and ch:
                self.release(conn, ch)

    def requeue_with_promotion(
        self,
        message_body: dict,
        new_priority: int,
    ) -> bool:
        task_id = message_body.get("task_id")
        original_priority = message_body.get("original_priority", message_body.get("priority", 1))
        enqueue_at = message_body.get("enqueue_at", time.time())

        promoted_count = message_body.get("promoted_count", 0) + 1

        logger.warning(
            "Aging promotion: task %s priority %d -> %d (promoted %d times, waited %.1fs)",
            task_id,
            original_priority,
            new_priority,
            promoted_count,
            time.time() - enqueue_at,
        )

        updated_message = dict(message_body)
        updated_message["priority"] = new_priority
        updated_message["promoted_count"] = promoted_count

        return self.publish_message(
            task_id=task_id,
            sender=updated_message["sender"],
            recipient=updated_message["recipient"],
            subject=updated_message["subject"],
            content=updated_message["content"],
            priority=new_priority,
            original_priority=original_priority,
            enqueue_at=enqueue_at,
        )

    def get_queue_depth(self) -> tuple[int, int]:
        conn = None
        ch = None
        try:
            conn, ch = self.acquire()
            q = ch.queue_declare(
                queue=self.queue_name,
                durable=True,
                arguments={"x-max-priority": 5},
                passive=True,
            )
            message_count = q.method.message_count
            consumer_count = q.method.consumer_count
            return message_count, consumer_count
        except Exception as e:
            logger.warning("Failed to get queue depth: %s", e)
            return -1, -1
        finally:
            if conn and ch:
                self.release(conn, ch)

    def close_all(self):
        while not self._pool.empty():
            try:
                conn, ch = self._pool.get_nowait()
                try:
                    ch.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
            except Empty:
                break
        self._initialized = False
        logger.info("RabbitMQ connection pool closed")


rabbitmq_pool = RabbitMQPool()
