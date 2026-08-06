from pydantic import BaseModel
from dags.storage import Event, ApplicationDatabaseConnection


def convert_to_Event_object(event_items: list[BaseModel]) -> list[Event]:
    return [
        Event(
            id=item.id,
            title=item.title,
            content=item.content,
            participants=item.participants,
            person_in_charge=item.person_in_charge,
            contact=item.contact,
            location=item.location,
            notes=item.notes,
            start_at=item.start_at,
            end_at=item.end_at,
        )
        for item in event_items
    ]


async def load_to_database(
    database_connection: ApplicationDatabaseConnection, data: list[BaseModel]
):
    events = convert_to_Event_object(event_items=data)
    result = await database_connection.load_to_table(data=events)

    return result


__all__ = ["load_to_database"]
