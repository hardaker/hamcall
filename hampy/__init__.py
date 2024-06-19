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

    parser.add_argument("-l", "--load", default=None, type=str,
                        help="Load this directory of files into the database")

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

def get_db(args):
    return sqlite3.connect(args.database)

def create_db(args):
    db = get_db(args)

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

    db.execute("""create table PUBACC_EN
(
      record_type               char(2)              not null,
      unique_system_identifier  numeric(9,0)         not null,
      uls_file_number           char(14)             null,
      ebf_number                varchar(30)          null,
      call_sign                 char(10)             null,
      entity_type               char(2)              null,
      licensee_id               char(9)              null,
      entity_name               varchar(200)         null,
      first_name                varchar(20)          null,
      mi                        char(1)              null,
      last_name                 varchar(20)          null,
      suffix                    char(3)              null,
      phone                     char(10)             null,
      fax                       char(10)             null,
      email                     varchar(50)          null,
      street_address            varchar(60)          null,
      city                      varchar(20)          null,
      state                     char(2)              null,
      zip_code                  char(9)              null,
      po_box                    varchar(20)          null,
      attention_line            varchar(35)          null,
      sgin                      char(3)              null,
      frn                       char(10)             null,
      applicant_type_code       char(1)              null,
      applicant_type_other      char(40)             null,
      status_code               char(1)          null,
      status_date       datetime        null,
      lic_category_code char(1)     null,
      linked_license_id numeric(9,0)    null,
      linked_callsign       char(10)        null
)""")
    db.execute("""create table PUBACC_HD
(
      record_type               char(2)              not null,
      unique_system_identifier  numeric(9,0)         not null,
      uls_file_number           char(14)             null,
      ebf_number                varchar(30)          null,
      call_sign                 char(10)             null,
      license_status            char(1)              null,
      radio_service_code        char(2)              null,
      grant_date                char(10)             null,
      expired_date              char(10)             null,
      cancellation_date         char(10)             null,
      eligibility_rule_num      char(10)             null,
      applicant_type_code_reserved       char(1)              null,
      alien                     char(1)              null,
      alien_government          char(1)              null,
      alien_corporation         char(1)              null,
      alien_officer             char(1)              null,
      alien_control             char(1)              null,
      revoked                   char(1)              null,
      convicted                 char(1)              null,
      adjudged                  char(1)              null,
      involved_reserved         char(1)              null,
      common_carrier            char(1)              null,
      non_common_carrier        char(1)              null,
      private_comm              char(1)              null,
      fixed                     char(1)              null,
      mobile                    char(1)              null,
      radiolocation             char(1)              null,
      satellite                 char(1)              null,
      developmental_or_sta      char(1)              null,
      interconnected_service    char(1)              null,
      certifier_first_name      varchar(20)          null,
      certifier_mi              char(1)              null,
      certifier_last_name       varchar(20)          null,
      certifier_suffix          char(3)              null,
      certifier_title           char(40)             null,
      gender                    char(1)              null,
      african_american          char(1)              null,
      native_american           char(1)              null,
      hawaiian                  char(1)              null,
      asian                     char(1)              null,
      white                     char(1)              null,
      ethnicity                 char(1)              null,
      effective_date            char(10)             null,
      last_action_date          char(10)             null,
      auction_id                int                  null,
      reg_stat_broad_serv       char(1)              null,
      band_manager              char(1)              null,
      type_serv_broad_serv      char(1)              null,
      alien_ruling              char(1)              null,
      licensee_name_change  char(1)          null,
      whitespace_ind            char(1)              null,
      additional_cert_choice    char(1)              null,
      additional_cert_answer    char(1)              null,
      discontinuation_ind       char(1)              null,
      regulatory_compliance_ind char(1)              null,
      eligibility_cert_900        char(1)              null,
      transition_plan_cert_900    char(1)              null,
      return_spectrum_cert_900  char(1)              null,
      payment_cert_900        char(1)              null)""")

    info(f"created database table at {args.database}")
    
def load_file_into_table(db, filename: str, table: str, columns: int):
    values = ("?, " * (columns-1)) + "?"
    with open(filename) as lh:
        for n, line in enumerate(lh):
            pieces = line.strip().split("|")
            db.execute(f"insert into PUBACC_{table} values({values})", pieces)
            if n % 10000 == 0:
                print(f"loading table {table}: {n}")
    db.commit()

def load_db(args):
    db = get_db(args)
    dir = Path(args.load)
    load_file_into_table(db, dir.joinpath("AM.dat"), "AM", 18)
    load_file_into_table(db, dir.joinpath("EN.dat"), "EN", 30)
    load_file_into_table(db, dir.joinpath("HD.dat"), "HD", 59)
    db.commit()

def main():
    args = parse_args()

    if args.init:
        create_db(args)
        return

    if args.load:
        load_db(args)
        

if __name__ == "__main__":
    main()
