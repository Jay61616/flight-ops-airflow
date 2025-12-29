import pandas as pd
import snowflake.connector
from airflow.hooks.base import BaseHook

def load_gold_to_snowflake(**context):
    gold_file = context["ti"].xcom_pull(
        key="gold_file",
        task_ids="gold_aggregate"
    )

    if not gold_file:
        raise ValueError("Gold file not found in xcom")
    
    execution_date = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
    
    df = pd.read_csv(gold_file)

    conn = BaseHook.get_connection("flight_snowflake")

    sf_conn = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson["account"],
        warehouse=conn.extra_dejson.get("warehouse"),
        database=conn.extra_dejson.get("database"),
        schema=conn.schema,
        role=conn.extra_dejson.get("role")
    )

    merge_sql = """
        MERGE INTO FLIGHT_KPIS tgt
        USING (
            SELECT
                TO_TIMESTAMP(%s) AS WINDOW_START,
                %s AS ORIGIN_COUNTRY,
                %s AS TOTAL_FLIGHTS,
                %s AS AVG_VELOCITY,
                %s AS ON_GROUND
        ) src
        ON tgt.WINDOW_START = src.WINDOW_START
           AND tgt.ORIGIN_COUNTRY = src.ORIGIN_COUNTRY
        WHEN MATCHED THEN UPDATE SET
            TOTAL_FLIGHTS = src.TOTAL_FLIGHTS,
            AVG_VELOCITY = src.AVG_VELOCITY,
            ON_GROUND = src.ON_GROUND,
            LOAD_TIME = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
        (WINDOW_START, ORIGIN_COUNTRY, TOTAL_FLIGHTS, AVG_VELOCITY, ON_GROUND)
        VALUES
        (src.WINDOW_START, src.ORIGIN_COUNTRY, src.TOTAL_FLIGHTS, src.AVG_VELOCITY, src.ON_GROUND);
    """

    with sf_conn.cursor() as cursor:
        for _, row in df.iterrows():
            cursor.execute(
                merge_sql,
                (
                    execution_date,
                    row["origin_country"],
                    int(row["total_flights"]),
                    float(row["avg_velocity"]),
                    int(row["on_ground"]),
                ),
            )

    sf_conn.close()