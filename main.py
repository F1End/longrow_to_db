from src.util import parse_args, parse_yaml, init_logging
from src.executor import ETLExecutor
from src.sparkutil import config
from src.db_tools import BuildDB

if __name__ == "__main__":
    args = parse_args()
    init_logging(args.debug)
    job_config = parse_yaml(args.job_config)
    if args.init_db:
        db_schema = parse_yaml(args.init_db)
        BuildDB(path=args.db_path, schema=db_schema).run()
    executor = ETLExecutor(args.data_file, job_config, config)
    executor.run_all()
