from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from models.task import MailTask, TaskStatus


def create_task(
    db: Session,
    sender: str,
    recipient: str,
    subject: str,
    content: str,
    priority: int,
) -> MailTask:
    task = MailTask(
        sender=sender,
        recipient=recipient,
        subject=subject,
        content=content,
        priority=priority,
        status=TaskStatus.PENDING,
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
) -> Optional[MailTask]:
    task = db.query(MailTask).filter(MailTask.id == task_id).first()
    if not task:
        return None
    task.status = status
    if error_message is not None:
        task.error_message = error_message
    if sent_at is not None:
        task.sent_at = sent_at
    task.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(task)
    return task
