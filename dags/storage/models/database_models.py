import json
from uuid import UUID
from typing import Any, Dict

from sqlalchemy import Text, ForeignKey, UUID as PG_UUID
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all database models."""

    pass


class Event(Base):
    """
    Represents a check-in event.

    Attributes:
        id (str): Unique identifier for the event (e.g., hashed file name or path).
        title (str): Title of the event.
        time (str): Textual representation of the event's timing.
        content (str): Detailed description of the event.
        participants (str): List or description of participants.
        person_in_charge (str): Person responsible for the event.
        contact (str): Contact information for the person in charge.
        location (str): Location where the event takes place.
        notes (str): Additional notes (e.g., dress code).
        start_at (datetime): Start timestamp of the event.
        end_at (datetime): End timestamp of the event.
    """

    __tablename__ = "events"
    __table_args__ = {"schema": "Events"}

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=True)
    participants: Mapped[str] = mapped_column(Text, nullable=True)
    person_in_charge: Mapped[str] = mapped_column(Text, nullable=True)
    contact: Mapped[str] = mapped_column(Text, nullable=True)
    location: Mapped[str] = mapped_column(Text, nullable=True)
    notes: Mapped[str] = mapped_column(Text, nullable=True)
    start_at: Mapped[str] = mapped_column(Text, nullable=False)
    end_at: Mapped[str] = mapped_column(Text, nullable=False)

    def __repr__(self) -> str:
        data = {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }
        return f"Event({json.dumps(data, default=str)})"


class Attendance(Base):
    """
    Represents attendance records for an event.

    Attributes:
        record_id (UUID): Unique identifier for the record.
        event_id (str): Foreign key referencing the event.
        attendees (Dict[str, Any]): JSONB dictionary of attendees.
    """

    __tablename__ = "attendance"
    __table_args__ = {"schema": "Events"}

    record_id: Mapped[UUID] = mapped_column(PG_UUID, primary_key=True)
    event_id: Mapped[str] = mapped_column(
        Text, ForeignKey("Events.events.id"), nullable=False
    )
    attendees: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict)

    def __repr__(self) -> str:
        data = {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }
        return f"Attendance({json.dumps(data, default=str)})"
