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

    class Config:
        env_file = ".env"


settings = Settings()
