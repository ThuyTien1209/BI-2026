import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DB_URL = os.getenv("SUPABASE_DB_URL")

if not DB_URL:
    raise ValueError(" Missing SUPABASE_DB_URL in .env")

engine = create_engine(
    DB_URL,
    pool_pre_ping=True,   # tránh connection chết
    pool_size=5,
    max_overflow=10
)