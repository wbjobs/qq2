from datetime import datetime
from typing import List, Optional

from sqlalchemy.orm import Session

from models.task import MailTask, TaskStatus


def create_task(
    db: Session,
    sender: str,
    recipient: str,
    subject: str,
    content: str,
    priority: int,
    scheduled_at: Optional[datetime] = None,
    max_retries: int = 3,
) -> MailTask:
    initial_status = TaskStatus.SCHEDULED if scheduled_at else TaskStatus.PENDING
    task = MailTask(
        sender=sender,
        recipient=recipient,
        subject=subject,
        content=content,
        priority=priority,
        status=initial_status,
        scheduled_at=scheduled_at,
        retry_count=0,
        max_retries=max_retries,
    )
    db.add(task)
    db.commit()
    db.refresh(task)
    return task


def get_task_by_id(db: Session, task_id: str) -> Optional[MailTask]:
    return db.query(MailTask).filter(MailTask.id == task_id).first()


def update_task_status(
    db: Session,
    task_id: str,
    status: TaskStatus,
    error_message: Optional[str] = None,
    sent_at: Optional[datetime] = None,
    retry_count: Optional[int] = None,
    next_retry_at: Optional[datetime] = None,
) -> Optional[MailTask]:
    task = db.query(MailTask).filter(MailTask.id == task_id).first()
    if not task:
        return None
    task.status = status
    if error_message is not None:
        task.error_message = error_message
    if sent_at is not None:
        task.sent_at = sent_at
    if retry_count is not None:
        task.retry_count = retry_count
    if next_retry_at is not None:
        task.next_retry_at = next_retry_at
    task.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(task)
    return task


def fetch_due_scheduled_tasks(db: Session, limit: int = 100) -> List[MailTask]:
    now = datetime.utcnow()
    tasks = (
        db.query(MailTask)
        .filter(
            MailTask.status == TaskStatus.SCHEDULED,
            MailTask.scheduled_at.isnot(None),
            MailTask.scheduled_at <= now,
        )
        .order_by(MailTask.scheduled_at.asc())
        .limit(limit)
        .all()
    )
    return tasks


def fetch_due_retry_tasks(db: Session, limit: int = 100) -> List[MailTask]:
    now = datetime.utcnow()
    tasks = (
        db.query(MailTask)
        .filter(
            MailTask.status == TaskStatus.RETRYING,
            MailTask.next_retry_at.isnot(None),
            MailTask.next_retry_at <= now,
        )
        .order_by(MailTask.next_retry_at.asc())
        .limit(limit)
        .all()
    )
    return tasks


def mark_task_enqueued(db: Session, task_id: str) -> bool:
    task = db.query(MailTask).filter(MailTask.id == task_id).first()
    if not task:
        return False
    task.status = TaskStatus.PENDING
    task.updated_at = datetime.utcnow()
    db.commit()
    return True
