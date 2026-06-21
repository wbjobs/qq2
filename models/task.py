import uuid
from datetime import datetime
from enum import Enum as PyEnum

from sqlalchemy import Column, String, Text, Integer, DateTime, Enum

from models.database import Base


class TaskStatus(str, PyEnum):
    PENDING = "pending"
    SCHEDULED = "scheduled"
    PROCESSING = "processing"
    RETRYING = "retrying"
    SUCCESS = "success"
    FAILED = "failed"


class MailTask(Base):
    __tablename__ = "mail_tasks"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    sender = Column(String(255), nullable=False)
    recipient = Column(String(255), nullable=False)
    subject = Column(String(512), nullable=False)
    content = Column(Text, nullable=False)
    priority = Column(Integer, nullable=False)
    status = Column(Enum(TaskStatus), default=TaskStatus.PENDING, nullable=False)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    sent_at = Column(DateTime, nullable=True)
    scheduled_at = Column(DateTime, nullable=True)
    retry_count = Column(Integer, default=0, nullable=False)
    max_retries = Column(Integer, default=3, nullable=False)
    next_retry_at = Column(DateTime, nullable=True)
