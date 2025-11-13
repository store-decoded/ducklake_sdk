import argparse
import os
from lake.connector.core import DuckLakeManager
from lake.util.logger import logger


def main():
    """
    Main function to set up and parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        prog="integrator(lake)",  # The name of your CLI tool
        description="Ducklake(DataLake) + Bi Dashboards(Panel)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0", # Fetches version from prog
        help="Show program's version number and exit.",
    )

    subparsers = parser.add_subparsers(
        title="Commands",
        dest="command",
        help="Available sub-commands",
        required=True, # Ensures a subcommand is always provided
    )
    parser_serve = subparsers.add_parser(
        "serve",
        help="execute a custom command on lake",
    )
    parser_test = subparsers.add_parser(
        "test",
        help="test some custom code-block",
    )
    parser_test.add_argument(
        "--config",
        "-c",
        type=str,
        required=True,
        default='resources/config.yml',
        help="path to config file included SRC/DEST"
    )
    parser_attach = subparsers.add_parser(
        "attach",
        help="attack ducklake to message broker",
    )
    parser_attach.add_argument(
        "--config",
        "-c",
        type=str,
        required=True,
        default='resources/config.yml',
        help="path to config file included SRC/DEST"
    )
    args = parser.parse_args()
    if args.command == 'attach':
        cnn = load_lake("kafka",args.config)
        cnn.attach()
    elif args.command == 'serve':
        from lake.connector import load_lake
        from lake.render import serve
        serve()
    elif args.command == 'test':
        lake = DuckLakeManager(args.config)

        # get_data = f"select * from lake.awesome_table;" 
        # result = lake.duckdb_connection.execute(get_data).fetch_df()
        # logger.info(result)




if __name__ == "__main__":
    main()


