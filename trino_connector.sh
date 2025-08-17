#!/bin/bash

docker exec -it trino-demo trino


# FOR DEMO
# -- Show your schema
# SHOW SCHEMAS FROM memory;

# -- Show tables inside your demo schema
# SHOW TABLES FROM memory.demo;

# -- Inspect the structure of your table
# DESCRIBE memory.demo.test_table;

# -- See your inserted rows
# SELECT * FROM memory.demo.test_table;