import os
from dotenv import load_dotenv
from pyflink.table import TableEnvironment, EnvironmentSettings

def get_jars_path():
    path = os.getcwd()
    return f'file://{path}/jars/kafka_jars/'

def get_env(key:str, default:str) -> str: 
  return os.getenv(key, default)

def get_jars_full_path() -> str:
  jars_path = get_jars_path()
  jars = [
    'flink-sql-connector-kafka-1.16.0.jar;',
    'flink-sql-avro-1.16.0.jar;',
    'flink-sql-avro-confluent-registry-1.16.0.jar'
  ]
  full_str = ''
  for jar in jars:
    full_str = f'{full_str}{jars_path}{jar}'
  return full_str

def get_table_creation_string():
    table_name = get_env('TOPIC', 'my_topic')
    return f"""
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
                'topic' = '{table_name}'
            )
            """
  

def log_processing():
    env_settings = EnvironmentSettings.new_instance() \
      .with_built_in_catalog_name(get_env('CATALOG_NAME', 'my_catalog')) \
      .with_built_in_database_name(get_env('DATABASE_NAME', 'my_database')) \
      .in_streaming_mode().build()
  
    t_env = TableEnvironment.create(env_settings)
    
    t_env.get_config().set('pipeline.jars',get_jars_full_path()) \
      .set("parallelism.default", get_env('PARALLELISM', '1'))

    source_ddl = get_table_creation_string()
    t_env.execute_sql(source_ddl)
    table = t_env.from_path(get_env('TOPIC', 'my_topic'))
    table.execute().print()

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
  # t_env.execute_sql(sink_ddl)
  # t_env.sql_query("SELECT a FROM source_table") \
  #     .execute_insert("sink_table").wait()


if __name__ == '__main__':
    load_dotenv()
    log_processing()