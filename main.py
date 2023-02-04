import os
from dotenv import load_dotenv
from pyflink.table import TableEnvironment, EnvironmentSettings

def get_jars_path():
    path = os.getcwd()
    return f'file://{path}/jars/kafka_jars'

def log_processing():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    jars_path = get_jars_path()
    t_env.get_config().set(
      'pipeline.jars',
      f'{jars_path}/flink-sql-connector-kafka-1.16.0.jar;{jars_path}/flink-sql-avro-1.16.0.jar;{jars_path}/flink-sql-avro-confluent-registry-1.16.0.jar'
    )
    table_name = os.getenv('TOPIC')
    source_ddl = f"""
            CREATE TABLE {table_name} (
                token VARCHAR
            ) WITH (
                'avro-confluent.basic-auth.credentials-source' = 'USER_INFO',
                'avro-confluent.basic-auth.user-info' = '{os.getenv('SCHEMA_REGISTRY_API_KEY')}',
                'avro-confluent.subject' = '{os.getenv('TOPIC')}-value',
                'avro-confluent.url' = '{os.getenv('SCHEMA_REGISTRY_URL')}',
                'connector' = 'kafka',
                'format' = 'avro-confluent',
                'properties.bootstrap.servers' = '{os.getenv('BOOTSTRAP_SERVERS')}',
                'properties.group.id' = 'pytflink_demo_joins_v{os.getenv('CONSUMER_GROUP_VERSION')}',
                'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv('KAFKA_USER')}" password="{os.getenv('KAFKA_PASSWORD')}";',
                'properties.sasl.mechanism' = 'PLAIN',
                'properties.security.protocol' = 'SASL_SSL',
                'scan.startup.mode' = 'latest-offset',
                'topic' = '{os.getenv('TOPIC')}'
            )
            """

    # sink_ddl = """
    #         CREATE TABLE sink_table(
    #             a VARCHAR
    #         ) WITH (
    #           'connector' = 'kafka',
    #           'topic' = 'sink_topic',
    #           'properties.bootstrap.servers' = 'kafka:9092',
    #           'format' = 'json'
    #         )
    #         """

    t_env.execute_sql(source_ddl)
    table = t_env.from_path(table_name)
    table.execute().print()

    # t_env.execute_sql(sink_ddl)

    # t_env.sql_query("SELECT a FROM source_table") \
    #     .execute_insert("sink_table").wait()


if __name__ == '__main__':
    load_dotenv()
    log_processing()