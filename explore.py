import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    from sqlalchemy import create_engine, text
    from dotenv import load_dotenv
    import os

    # Load your existing .env file — no need to hardcode credentials
    load_dotenv()

    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5433")
    DB_NAME = os.getenv("DB_NAME", "garage_monitor")
    DB_USER = os.getenv("DB_USER", "garage_user")
    DB_PASS = os.getenv("DB_PASS")

    # Build the connection string
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    mo.md("## 🚗 Garage Monitor — Data Explorer")
    return engine, mo, pd, text


@app.cell
def _(engine, pd, text):
    # Load the most recent 100 events from the view
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT * FROM v_door_events ORDER BY received_at DESC LIMIT 100"),
            conn
        )

    df
    return (df,)


@app.cell
def _(df, mo):
    mo.md(f"""
    **Total rows loaded:** {len(df)}
    """)
    return


if __name__ == "__main__":
    app.run()
