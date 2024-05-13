import os
from configparser import ConfigParser

class ConfigurationReader():

    def __init__(self) -> None:
        env_file_path = self.get_file_path('env.ini')
        
        if not os.path.isfile(env_file_path):
            raise FileNotFoundError(f"The properties file '{env_file_path}' does not exist.")
        env_config = ConfigParser()
        env_config.read(env_file_path)
        self.environment = env_config.get('Environment', 'env')

    def get_config(self):
        config_file_path = self.get_file_path(f"{self.environment}.ini")

        if not os.path.isfile(config_file_path):
            raise FileNotFoundError(f"The properties file {config_file_path} does not exist.")
        
        config = ConfigParser()
        config.read(config_file_path)

        return config
    
    def get_clickhouse_config(self):
        config = self.get_config()
        section = 'Clickhouse'
        host = config.get(section, 'host')
        port = config.get(section, 'port')
        database = config.get(section, 'database')
        user = config.get(section, 'user')
        password = config.get(section, 'password')

        return host, port, database, user, password

    def get_file_path(self, file_name):
        print('Generating path for file')
        print(file_name)
        script_directory = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(script_directory, file_name)