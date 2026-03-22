from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_session_sink_postgres(t_env):
    table_name = 'processed_events_session'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end   TIMESTAMP(3),
            pulocationid INT,
            num_trips    BIGINT,
            PRIMARY KEY (window_start, window_end, pulocationid) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    table_name = "green_events_session"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def run_session_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Advance watermark after source goes idle so last open session fires
    t_env.get_config().set("table.exec.source.idle-timeout", "10 s")

    try:
        source_table = create_events_source_kafka(t_env)
        sink_table = create_session_sink_postgres(t_env)

        t_env.execute_sql(f"""
            INSERT INTO {sink_table}
            SELECT
                window_start,
                window_end,
                PULocationID AS pulocationid,
                COUNT(*) AS num_trips
            FROM TABLE(
                SESSION(
                    TABLE {source_table} PARTITION BY PULocationID,
                    DESCRIPTOR(event_timestamp),
                    INTERVAL '5' MINUTE
                )
            )
            GROUP BY window_start, window_end, PULocationID
        """).wait()

    except Exception as e:
        print("Session window job failed:", str(e))


if __name__ == '__main__':
    run_session_job()
