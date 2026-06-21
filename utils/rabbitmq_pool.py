import json
import logging
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
