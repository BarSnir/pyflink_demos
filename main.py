import os, json
from dotenv import load_dotenv
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table import expressions as F
from src.libs.connectors import Connectors 

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
    'flink-sql-avro-confluent-registry-1.16.0.jar;',
    'flink-connector-jdbc-1.16.0.jar;',
    'mysql-connector-java-5.1.9.jar'
  ]
  full_str = ''
  for jar in jars:
    full_str = f'{full_str}{jars_path}{jar}'
  return full_str

def log_processing():
    config = open('config.json')
    connectors = Connectors(json.load(config))
    mysql_source_ddls =  connectors.get_connector_config('mysql_source')
    kafka_source_ddls = connectors.get_connector_config('kafka_source')
    kafka_sink_ddls = connectors.get_connector_config('kafka_sink')

    env_settings = EnvironmentSettings.new_instance() \
      .with_built_in_catalog_name(get_env('CATALOG_NAME', 'my_catalog')) \
      .with_built_in_database_name(get_env('DATABASE_NAME', 'my_database')) \
      .in_streaming_mode().build()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set('pipeline.jars',get_jars_full_path()) \
      .set("parallelism.default", get_env('PARALLELISM', '6'))

    for ddl in mysql_source_ddls:
      t_env.execute_sql(ddl)

    for ddl in kafka_source_ddls:
      t_env.execute_sql(ddl)

    for ddl in kafka_sink_ddls:
      t_env.execute_sql(ddl)

if __name__ == '__main__':
    load_dotenv()
    log_processing()