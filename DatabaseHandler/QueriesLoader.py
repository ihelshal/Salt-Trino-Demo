# In[]: Import Packages

from typing import List

import yaml

from Configurations import db_config as config

# In[]: Load the YAML file from config Directory (temp.yaml, GoldenKPIs.yaml)


class QueriesLoader:
    def __init__(self, file_name: str) -> None:
        self.file_name = file_name

    def safe_load(self) -> List[str]:
        # Load the YAML file
        with open(config.queries_playbook, "r") as file:
            queries = yaml.safe_load(file)
        return queries

    def get_config_by_name(self, config, name):
        for entry in config:
            if entry["name"] == name:
                return entry
        raise ValueError(f"No configuration found for name: {name}")
