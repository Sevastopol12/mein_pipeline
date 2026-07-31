from pydantic import BaseModel, Field


class Department(BaseModel):
    """Represents a department within the organization.

    Attributes:
        id (str): Unique identifier for the department (e.g., 'dept_1').
        name (str): Display name of the department (e.g., 'Department 1').
    """

    id: str
    name: str


class AttendanceRecord(BaseModel):
    """Represents an attendance count for a specific event and department.

    Attributes:
        event_id (str): Unique identifier of the event.
        department_id (str): Unique identifier of the department.
        count (int): Number of people present.
    """

    event_id: str
    department_id: str
    count: int


class EventItem(BaseModel):
    id: str = Field(
        description="The unique identifier for the document, which is the hashed file name or its uploaded path."
    )

    is_event: bool = Field(
        description="True if the document contains details about an event hosted by a person or organization; False otherwise."
    )
    title: str = Field(description="The main title or name of the event.")

    content: str = Field(
        description="Full file content. Must be formatted in Markdown (.md)."
    )
    participants: str = Field(
        description="The list of participants, formatted in Markdown (.md) (e.g., as a bulleted list)."
    )
    person_in_charge: str = Field(
        description="The person or people responsible/in charge."
    )
    contact: str = Field(
        description="Phone number / how to contact the person or people responsible/in charge."
    )
    location: str = Field(
        description="The location, address of where the event would take place."
    )
    notes: str = Field(
        description="The note on what to wear if specifically mentioned and formatted in Markdown (.md) (e.g., as a bulleted list), otherwise empty."
    )
    start_at: str = Field(
        description="The start date and time of the event. Must be in ISO 8601 format (e.g., YYYY-MM-DDTHH:MM:SSZ)."
    )
    end_at: str = Field(
        description="The end date and time of the event. Must be in ISO 8601 format (e.g., YYYY-MM-DDTHH:MM:SSZ)."
    )


class ParquetTable(BaseModel):
    format: str
    encoded_bytes: str

    @classmethod
    def get_template(cls) -> dict[str, list]:
        return {attr: [] for attr in cls.model_fields.keys()}
