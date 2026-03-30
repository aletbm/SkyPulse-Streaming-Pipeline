import os

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        port=int(os.getenv("SUPABASE_PORT", 5432)),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        dbname=os.getenv("SUPABASE_DATABASE", "postgres"),
        sslmode="require",
        connect_timeout=10,
    )


def query(sql: str, params=None) -> pd.DataFrame:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    except Exception:
        conn.rollback()
        raise
