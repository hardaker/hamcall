"""Create and access a local FCC ULS database of ham callsigns"""

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, FileType
from logging import debug, info, warning, error, critical
import logging
import sys
import sqlite3
from pathlib import Path

# optionally use rich
try:
    from rich import print
    from rich.logging import RichHandler
except Exception:
    pass

# optionally use rich_argparse too
help_handler = ArgumentDefaultsHelpFormatter
try:
    from rich_argparse import RichHelpFormatter
    help_handler = RichHelpFormatter
except Exception:
    pass

def parse_args():
    "Parse the command line arguments."
    parser = ArgumentParser(formatter_class=help_handler,
                            description=__doc__,
                            epilog="Example Usage: ")

    parser.add_argument("-d", "--database", default=Path.home()  / ".hampy.db",
                        type=str,
                        help="Where to store the database")

    parser.add_argument("-i", "--init", action="store_true",
                        help="Create the database structure.")

    parser.add_argument("--log-level", "--ll", default="info",
                        help="Define the logging verbosity level (debug, info, warning, error, fotal, critical).")

    parser.add_argument("input_file", type=FileType('r'),
                        nargs='?', default=sys.stdin,
                        help="")

    parser.add_argument("output_file", type=FileType('w'),
                        nargs='?', default=sys.stdout,
                        help="")

    args = parser.parse_args()
    log_level = args.log_level.upper()
    handlers = []
    datefmt = None
    messagefmt = "%(levelname)-10s:\t%(message)s"

    # see if we're rich
    try:
        handlers.append(RichHandler(rich_tracebacks=True))
        datefmt = " "
        messagefmt = "%(message)s"
    except Exception:
        pass

    logging.basicConfig(level=log_level,
                        format=messagefmt,
                        datefmt=datefmt,
                        handlers=handlers)
    return args

def main():
    args = parse_args()

    if args.init:
        print(args.database)
        db = sqlite3.connect(args.database)

        db.execute("""create table PUBACC_AM
(
      record_type               char(2)              not null,
      unique_system_identifier  numeric(9,0)         not null,
      uls_file_num              char(14)             null,
      ebf_number                varchar(30)          null,
      callsign                  char(10)             null,
      operator_class            char(1)              null,
      group_code                char(1)              null,
      region_code               tinyint              null,
      trustee_callsign          char(10)             null,
      trustee_indicator         char(1)              null,
      physician_certification   char(1)              null,
      ve_signature              char(1)              null,
      systematic_callsign_change char(1)             null,
      vanity_callsign_change    char(1)              null,
      vanity_relationship       char(12)             null,
      previous_callsign         char(10)             null,
      previous_operator_class   char(1)              null,
      trustee_name              varchar(50)          null
)""")
        info(f"created database table at {args.database}")


if __name__ == "__main__":
    main()
