import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from api.schemas import SendMailRequest, SendMailResponse, TaskStatusResponse
from models import get_db, crud
from utils import rabbitmq_pool

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post(
    "/send",
    response_model=SendMailResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="发送邮件任务",
    description="接收邮件参数，创建任务。若指定scheduled_at则定时发送，否则立即推送到RabbitMQ队列",
)
def send_mail(
    request: SendMailRequest,
    db: Session = Depends(get_db),
):
    task = crud.create_task(
        db=db,
        sender=request.sender,
        recipient=request.recipient,
        subject=request.subject,
        content=request.content,
        priority=request.priority,
        scheduled_at=request.scheduled_at,
    )

    if request.scheduled_at:
        return SendMailResponse(
            task_id=task.id,
            status=task.status.value,
            message=f"邮件任务已创建，将于 {request.scheduled_at.isoformat()} 发送",
        )

    published = rabbitmq_pool.publish_message(
        task_id=task.id,
        sender=task.sender,
        recipient=task.recipient,
        subject=task.subject,
        content=task.content,
        priority=task.priority,
    )

    if not published:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="消息队列服务不可用，请稍后重试",
        )

    return SendMailResponse(
        task_id=task.id,
        status=task.status.value,
        message="邮件任务已提交，正在排队处理",
    )


@router.get(
    "/status/{task_id}",
    response_model=TaskStatusResponse,
    summary="查询任务状态",
    description="根据任务ID查询邮件发送状态，包含重试和定时发送信息",
)
def get_task_status(
    task_id: str,
    db: Session = Depends(get_db),
):
    task = crud.get_task_by_id(db, task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="任务不存在",
        )

    def _fmt(dt: datetime | None) -> str | None:
        return dt.isoformat() if dt else None

    return TaskStatusResponse(
        task_id=task.id,
        sender=task.sender,
        recipient=task.recipient,
        subject=task.subject,
        priority=task.priority,
        status=task.status,
        error_message=task.error_message,
        created_at=_fmt(task.created_at),
        updated_at=_fmt(task.updated_at),
        sent_at=_fmt(task.sent_at),
        scheduled_at=_fmt(task.scheduled_at),
        retry_count=task.retry_count,
        max_retries=task.max_retries,
        next_retry_at=_fmt(task.next_retry_at),
    )
