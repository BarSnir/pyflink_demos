import os

class KafkaConfigs:

    def __init__(self):
        return

    def _get_sasl_string(self, KAFKA_USER, KAFKA_PASSWORD):
        return f'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'

    def get_kafka_source_default_config(self):
        SR_API_KEY = os.getenv('SCHEMA_REGISTRY_API_KEY')
        SR_URL = os.getenv("SCHEMA_REGISTRY_URL")
        BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')
        KAFKA_USER = os.getenv('KAFKA_USER')
        KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
        SASL_STRING = self._get_sasl_string(KAFKA_USER, KAFKA_PASSWORD)
        return {
            "'connector'":"'kafka',",
            "'format'": "'avro-confluent',",
            "'avro-confluent.basic-auth.credentials-source'": "'USER_INFO',",
            "'avro-confluent.basic-auth.user-info'": f"'{SR_API_KEY}',",
            "'avro-confluent.url'": f"'{SR_URL}',",
            "'properties.bootstrap.servers'": f"'{BOOTSTRAP_SERVERS}',",
            "'properties.sasl.jaas.config'": f"'{SASL_STRING}',",
            "'properties.sasl.mechanism'": "'PLAIN',",
            "'properties.security.protocol'": "'SASL_SSL',",
        }