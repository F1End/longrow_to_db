## longrow_to_db
Processing and persisting longrow data with Spark for wartracker project


## Contents
- [Description](#description)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)

## Description
Part of [Wartracker project](https://github.com/users/F1End/projects/1/views/1?pane=info).

The script uses csv files as input (from [parsehtml](https://github.com/F1End/parsehtml?tab=readme-ov-file)) and parses them into data tables using PySpark, updating an SQLite db file.
The script can initiate the db if it does not exists, including tables and indexing setup.

In current configuration it can persist losses only, but the setup is quite modular.

Mid-term plans include refactoring db structure to be more efficient.

Long-term plans include extending coverage to other resources, e.g. pledges and other loss documenting sites, and possibly moving to MySQL of PostgreSQL.


## Requirements
The script uses python 3.11 (due to PySpark compatibility), but likely will work with most earlier versions after 3.8, and can confirm it working with 3.12.3 on Ubuntu.

Spark and openjdk 17 (as Spark uses JVM) needs to be installed (may work with 8 or 11 too). I suggest consulting the [compatibility matrix](https://sparkbyexamples.com/spark/spark-versions-supportability-matrix/)

The external library requirements are pyspark, pandas, py4j, numpy and PyYAML.


## Installation

1. Git clone repo
2. Install openjdk 17 (Needed as spark runs on JVM; might also work with older version)
3. Install Spark 3.5.5
4. Install requirements (and create venv if needed)
5. You may need to configure JAVA_HOME and SPARK_HOME, especially if you intend to run it in a container.

## Usage
The script has a some mandatory and some optional flags. The second group is either due to optionality in execution or due to falling back to default value.

Mandatory:<br>
--data_file: The input csv's path must follow <br>
--job_config: Yaml configuration for the executable etl job(s). Currently one working example exists in as ./config/oryxloss.yaml<br>
--db_path OR --out_dir: At least one of these must be specified so data can be saved. The first one is path to .db file, second is name of output dir into which persist spark output.<br>

Optional:<br>
--base_config and --base_config_key: First is to enter yaml file path that holds spark settings that are run during initiation. Second is the key to pick up values from collection. If left empty, resolves to "./config/default_config.yaml" and "test"<br>
--init_db: path of desired db config must follow. When used, new db and the specified db settings will be initiated if they do not exist yet. Currently one config exists in./config/db/oryxloss_schema.yaml<br>
--debug: Set logger level to debug.<br>


Pattern:<br>

<python> main.py --base_config config/default_config.yaml --job_config config/oryxloss.yaml --init_db config/db/oryxloss_schema.yaml --db_path <path to something.db> --data_file <path_to_csv> --out_dir <output_path><br>

Sample command:<br>

python3 main.py --base_config config/default_config.yaml --job_config config/oryxloss.yaml --init_db config/db/oryxloss_schema.yaml --db_path ~/scripts/wartracker-frontend/data/wartracker.db --data_file /data/wartracker/stage/2025-04-11_attack-on-europe-documenting-equipment_parsed.csv --out_dir ~/data/wartracker/spark-out
