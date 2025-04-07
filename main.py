from src.util import parse_args, parse_yaml, init_logging
from src.executor import ETLExecutor
from src.db_tools import DBConn


if __name__ == "__main__":
    args = parse_args()
    init_logging(args.debug)
    job_config = parse_yaml(args.job_config)

    if args.base_config:
        config = parse_yaml(args.base_config)["configs"][args.base_config_key]

    if args.init_db:
        db_schema = parse_yaml(args.init_db)
        db_conn = DBConn(db_path=args.db_path)
        with db_conn as db_session:
            db_session.build_db(db_schema)

    if args.db_path:
        job_config["db"] = args.db_path
    if args.out_dir:
        job_config["out_dir"] = args.out_dir

    executor = ETLExecutor(args.data_file, job_config, config)
    executor.run_all()
