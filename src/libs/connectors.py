from src.configs.kafka_defaults import KafkaConfigs
from src.configs.mysql_defaults import MySQLConfigs

class Connectors:

    def __init__(self, config:dict):
        self.config = config
        self.kafka_config = KafkaConfigs()
        self.mysql_config = MySQLConfigs()
        self.connector_ctx = {
            'kafka_source': self.kafka_config.get_kafka_source_default_config,
            'mysql_source': self.mysql_config.get_mysql_source_default_config,
            'kafka_sink': self.kafka_config.get_kafka_sink_default_config
        }

    def get_connector_config(self, connector_name:str) -> list[dict] :
        ddls = []
        connector_config = self.config.get(f'{connector_name}')
        for table in connector_config:
            table_config = connector_config.get(table)
            fields_string = self.get_connector_fields(
                table_config.get('fields'), table
            )
            connector_config_string = self.get_technical_connector_spec(
                table_config, connector_name
            )
            ddls.append(fields_string+connector_config_string)
        return ddls

    def get_connector_fields(self, fields: list[str], table_name: str) -> str:
        fields_string = ''
        for field in fields:
            fields_string = fields_string + field
        return f"CREATE TABLE {table_name} ( {fields_string} ) "

    def get_technical_connector_spec(self, table_configs: dict, source: str) -> str:
        config_str = ''
        table_config = table_configs.get('configs')
        database_name = table_configs.get('database')
        default_config = self.connector_ctx.get(f'{source}')()
        if database_name:
            config_str = config_str + self.mysql_config.get_mysql_connection_str(database_name)
        for key in default_config:
            config_str = config_str + f'{key}={default_config.get(key)}'
        for key in table_config:
            config_str = config_str + f'{key}={table_config.get(key)}'

        return f"WITH ( {config_str} ) """