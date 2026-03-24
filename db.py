from sqlalchemy import create_engine, inspect, text
from config import DATABASE_URL

engine = create_engine(DATABASE_URL, future=True)

def get_inspector():
    return inspect(engine)

def list_tables():
    inspector = get_inspector()
    return sorted(inspector.get_table_names(schema="public"))

def table_columns(table_name: str):
    inspector = get_inspector()
    cols = inspector.get_columns(table_name, schema="public")
    return cols