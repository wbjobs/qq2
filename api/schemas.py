from pydantic import BaseModel, EmailStr, Field, field_validator

from models.task import TaskStatus


class SendMailRequest(BaseModel):
    sender: str = Field(..., min_length=1, max_length=255, description="发件人邮箱")
    recipient: str = Field(..., min_length=1, max_length=255, description="收件人邮箱")
    subject: str = Field(..., min_length=1, max_length=512, description="邮件主题")
    content: str = Field(..., min_length=1, description="邮件内容")
    priority: int = Field(..., ge=1, le=5, description="优先级 1-5，5为最高")

    @field_validator("priority")
    @classmethod
    def check_priority(cls, v: int) -> int:
        if v < 1 or v > 5:
            raise ValueError("优先级必须在 1 到 5 之间")
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
