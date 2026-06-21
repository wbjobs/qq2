from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://postgres:postgres@localhost:5432/mail_queue"

    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"
    RABBITMQ_VHOST: str = "/"
    RABBITMQ_QUEUE: str = "mail_queue"
    RABBITMQ_POOL_SIZE: int = 10

    NORMAL_PRIORITY_MAX_RATE: int = 10
    HIGH_PRIORITY_THRESHOLD: int = 4

    AGING_THRESHOLD_SECONDS: int = 300
    AGING_PROMOTION_PRIORITY: int = 5
    HIGH_PRIORITY_BACKLOG_THRESHOLD: int = 1000

    BATCH_UPDATE_ENABLED: bool = True
    BATCH_UPDATE_MAX_SIZE: int = 100
    BATCH_UPDATE_INTERVAL_MS: int = 500
    BATCH_UPDATE_MAX_WORKERS: int = 2

    class Config:
        env_file = ".env"


settings = Settings()
