import logging
import signal
import sys
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import router as api_router
from consumer import MailConsumer, task_scheduler
from models import init_db
from utils import rabbitmq_pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

_consumer: MailConsumer | None = None
_consumer_thread: threading.Thread | None = None


def _start_consumer():
    global _consumer, _consumer_thread
    _consumer = MailConsumer()
    _consumer_thread = threading.Thread(
        target=_consumer.start, daemon=True, name="mail-consumer"
    )
    _consumer_thread.start()
    logger.info("Mail consumer thread started")


def _stop_consumer():
    global _consumer
    if _consumer:
        _consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing database...")
    init_db()
    logger.info("Database initialized")

    logger.info("Initializing RabbitMQ connection pool...")
    rabbitmq_pool.initialize()
    logger.info("RabbitMQ pool initialized")

    logger.info("Starting mail consumer...")
    _start_consumer()

    logger.info("Starting task scheduler...")
    task_scheduler.start()

    yield

    logger.info("Stopping task scheduler...")
    task_scheduler.stop()

    logger.info("Shutting down mail consumer...")
    _stop_consumer()

    logger.info("Closing RabbitMQ connection pool...")
    rabbitmq_pool.close_all()
    logger.info("Shutdown complete")


app = FastAPI(
    title="Mail Queue Service",
    description="基于 RabbitMQ 的邮件发送队列服务，支持定时发送和指数退避重试",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, tags=["mail"])


@app.get("/health", summary="健康检查")
def health_check():
    return {"status": "ok", "service": "mail-queue"}


def _handle_signal(signum, frame):
    logger.info("Received signal %s, shutting down...", signum)
    task_scheduler.stop()
    _stop_consumer()
    rabbitmq_pool.close_all()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
