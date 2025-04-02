from src.util import parse_args, parse_yaml, init_logging
from src.executor import ETLExecutor
from src.sparkutil import config

if __name__ == "__main__":
    args = parse_args()
    init_logging(args.debug)
    job_config = parse_yaml(args.job_config)
    executor = ETLExecutor(args.data_file, job_config, config)
    executor.run_all()
