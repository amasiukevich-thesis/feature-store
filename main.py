import pandas as pd
import csv

import logging

from datetime import datetime, timedelta

import sqlalchemy.orm
from sqlalchemy import Column, BigInteger, String, TIMESTAMP, DECIMAL, create_engine, inspect, text
from sqlalchemy.orm import sessionmaker, declarative_base

from config import STUMP_PATH, URL, DATE_COL, TIME_AT_REFRESH

from src.incoming_data_pipeline import prepare_raw_data, check_interpolation_needed
from sqlalchemy import func

import time

### TODO: Refactor this all!!!



import os
import requests
import schedule


DEFAULT_DATE_VALUE = "2022-06-09 00:00:00"

COLUMNS_MAPPING = {
    "unix": "unix",
    "date": "rate_date",
    "symbol": "symbol",
    "open": "price_open",
    "close": "price_close",
    "low": "price_low",
    "high": "price_high",
    "Volume ETH": "volume_eth",
    "Volume USD": "volume_usd",
}

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


# SQLAlchemy stuff - leave them here, because the
Base = declarative_base()


connection_string = os.environ.get("CONNECTION_STRING")

engine = create_engine(connection_string, pool_size=10, max_overflow=0)

class RateUnit(Base):
    __tablename__ = "rates"

    unix = Column(BigInteger, autoincrement=False, primary_key=True)
    rate_date = Column(TIMESTAMP)
    symbol = Column(String)
    price_open = Column(DECIMAL)
    price_close = Column(DECIMAL)
    price_low = Column(DECIMAL)
    price_high = Column(DECIMAL)
    volume_eth = Column(DECIMAL)
    volume_usd = Column(DECIMAL)


if 'rates' not in [table.name for table in Base.metadata.sorted_tables]:
    Base.metadata.create_all(engine)


def only_select_needed_rows(
    response: requests.Response, path_to_write: str, date_thr: datetime, mode="continue"
) -> None:
    """
    :param response: a response from the server - getting the csv file
    :param path_to_write: a path to write the file
    :param date_thr: a date to filter the values by

    :returns None: writes the file to the path
    """

    with open(path_to_write, "w") as f:
        writer = csv.writer(f)
        date_idx = 0
        columns = []
        for i, line in enumerate(response.iter_lines()):
            # first line should be removed anyway, second is the header
            if i == 0:
                continue
            if i == 1:
                columns = line.decode("utf-8").split(",")
                date_idx = columns.index(DATE_COL)
                writer.writerow(line.decode("utf-8").split(","))
            if i > 1:
                row = line.decode("utf-8").split(",")
                date = row[date_idx]

                # TODO: rewrite this code with operator mapping
                if mode == "begin":
                    if datetime.strptime(date, DATE_FORMAT) <= date_thr:
                        writer.writerow(row)
                elif mode == "continue":
                    if datetime.strptime(date, DATE_FORMAT) > date_thr:
                        writer.writerow(row)

def update_db(orm_session: sqlalchemy.orm.Session, data: pd.DataFrame):
    """
    Updates the database by:

    1. Getting the data from the API
    2. Saving it in temporary CSV file
    3. Reading and selecting only needed columns
    4. Updating to the database

    """

    # TODO: refactor this to use the database engine
    data.to_sql("rates", engine, if_exists="append", index=False)

    orm_session.commit()

    # TODO: Logging here
    logging.info(f"Num entries inserted: {len(data)}")


def test_column_names(session: sqlalchemy.orm.Session):

    columns = set([column.key for column in inspect(RateUnit).column_attrs])
    columns_actual = set([
        "unix",
        "symbol",
        "price_open",
        "price_close",
        "price_low",
        "price_high",
        "volume_eth",
        "volume_usd",
        "rate_date",
    ])

    assert columns == columns_actual, "columns are wrong"

def read_from_db_and_sort(session: sqlalchemy.orm.Session) -> pd.DataFrame:
    """
    Reads from the database and returns the data as a dataframe

    :param session: an SQLAlchemy session

    :returns: Dataframe - the result of the query
    """

    all_data = pd.read_sql("SELECT * FROM rates ORDER BY rate_date DESC", con=engine)
    return all_data


def get_max_date(orm_session: sqlalchemy.orm.Session) -> str:
    """
    Returns maximum date from the database

    :param orm_session: established database session

    :returns: string representation of the date to share the formats
    """

    query_obj = orm_session.query(RateUnit)
    max_date_value = query_obj.with_entities(func.max(RateUnit.rate_date)).scalar()
    max_date_value = (
        datetime.strftime(max_date_value, DATE_FORMAT)
        if max_date_value
        else DEFAULT_DATE_VALUE
    )

    return max_date_value


def insert_rows(
    orm_session: sqlalchemy.orm.Session,
    response: requests.Response,
    mode: str = "continue",
) -> None:
    """
    Scheduled function

    Updates the database at the specific time at the day.

    :param orm_session:

    :returns None:
    """

    # TODO: reorganize the code to run checks first

    date_before = read_from_db_and_sort(orm_session)
    max_date = (
        date_before["rate_date"].max().strftime(DATE_FORMAT)
        if not date_before.empty
        else DEFAULT_DATE_VALUE
    )

    if datetime.strptime(max_date, DATE_FORMAT) > datetime.now() - timedelta(days=1):
        return

    only_select_needed_rows(
        response, STUMP_PATH, datetime.strptime(max_date, DATE_FORMAT), mode=mode
    )
    # already cut off to the max date in DB

    # TODO: data preparation - maybe into another function???
    incoming_data = pd.read_csv(STUMP_PATH)
    incoming_data = incoming_data.sort_values(DATE_COL, ascending=False)
    incoming_data = prepare_raw_data(incoming_data)
    incoming_data = incoming_data.rename(columns=COLUMNS_MAPPING)

    dataframe = pd.concat([incoming_data, date_before])

    check_needed, _ = check_interpolation_needed(dataframe)

    # TODO: What could go wrong here - possible scenarios
    assert not check_needed, "Interpolation needed in the database"

    update_db(orm_session, incoming_data)

    # TODO: logging here
    print("Successfully updated the data")


def make_base_db(
    orm_session: sqlalchemy.orm.Session, response: requests.Response
) -> None:
    """
    Starts the database
    """

    max_date = DEFAULT_DATE_VALUE
    insert_rows(orm_session, response, mode="begin")
    full_data = read_from_db_and_sort(orm_session)
    check_needed, idx_wrong = check_interpolation_needed(full_data)
    assert not check_needed, "Interpolation needed in the database"

    # TODO: logging here
    print("Successfully updated the data")


def schedule_update(session_class) -> None:
    # TODO: Specify in the parameters when do you want to schedule updates

    orm_session = session_class()
    # update flow
    response = requests.get(URL)
    insert_rows(orm_session, response, mode="continue")

    print("Successfully updated the database")


if __name__ == "__main__":

    print("Entering container")
    Session = sessionmaker(bind=engine)
    session = Session()

    test_column_names(session)

    response = requests.get(URL, timeout=300)

    print("Created incomplete_db")

    schedule.every().day.at(TIME_AT_REFRESH).do(lambda: schedule_update(Session))

    while True:
        schedule.run_pending()
        time.sleep(60)

    # TODO: think about how to schedule this procedure daily
    # TODO: think about how to test it when deployed
    # TODO: arrange the docker-compose.yml as you have in ChatGPT response - to deploy an embedded volume - database
