from typing import Optional
from datetime import datetime

from pydantic import BaseModel, Field, field_validator

from models.task import TaskStatus


class SendMailRequest(BaseModel):
    sender: str = Field(..., min_length=1, max_length=255, description="发件人邮箱")
    recipient: str = Field(..., min_length=1, max_length=255, description="收件人邮箱")
    subject: str = Field(..., min_length=1, max_length=512, description="邮件主题")
    content: str = Field(..., min_length=1, description="邮件内容")
    priority: int = Field(..., ge=1, le=5, description="优先级 1-5，5为最高")
    scheduled_at: Optional[datetime] = Field(
        None, description="定时发送时间（ISO 8601格式，精确到分钟），不填则立即发送"
    )

    @field_validator("priority")
    @classmethod
    def check_priority(cls, v: int) -> int:
        if v < 1 or v > 5:
            raise ValueError("优先级必须在 1 到 5 之间")
        return v

    @field_validator("scheduled_at")
    @classmethod
    def check_scheduled_at(cls, v: Optional[datetime]) -> Optional[datetime]:
        if v is not None and v < datetime.utcnow():
            raise ValueError("定时发送时间不能早于当前时间")
        return v


class SendMailResponse(BaseModel):
    task_id: str
    status: str
    message: str


class TaskStatusResponse(BaseModel):
    task_id: str
    sender: str
    recipient: str
    subject: str
    priority: int
    status: TaskStatus
    error_message: str | None = None
    created_at: str
    updated_at: str
    sent_at: str | None = None
    scheduled_at: str | None = None
    retry_count: int = 0
    max_retries: int = 3
    next_retry_at: str | None = None
