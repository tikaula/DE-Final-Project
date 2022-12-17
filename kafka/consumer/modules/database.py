from sqlalchemy import create_engine, text, engine


def get_engine(host: str, port: int, user: str, password: str):
    db_uri = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres"
    engine = create_engine(db_uri)
    return engine

def insert_data(engine: engine.Engine, all_data: dict, tablename: str):
    with engine.begin() as conn:
        conn.execute(
            text(f"""INSERT INTO {tablename} (currency_id, currency_name, rate, timestamp)
            VALUES (:currency_id, :currency_name, :rate, :timestamp)"""),
            [all_data]
        )