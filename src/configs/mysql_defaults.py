import os

class MySQLConfigs:

    def __init__(self):
        return

    def get_mysql_connection_str(self, database:str) -> str:
        DATABASE_HOST = os.getenv('DATABASE_HOST')
        return f"'url'='jdbc:mysql://{DATABASE_HOST}:3306/{database}',"

    def get_mysql_source_default_config(self):
        DATABASE_USER = os.getenv("DATABASE_USER")
        DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
        return {
            "'connector'":"'jdbc',",
            "'username'": f"'{DATABASE_USER}',",
            "'password'": f"'{DATABASE_PASSWORD}',"
        }