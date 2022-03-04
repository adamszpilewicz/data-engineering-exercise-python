import os

import pandas as pd
import pytest
from db import DB

@pytest.fixture(autouse=True)
def set_up_db_conn():
    os.remove("../db_test/db_test.sqlite")
    db = DB("../db_test/db_test.sqlite")
    return db

def test_connection_read_json(set_up_db_conn):
    df = set_up_db_conn._read_data_from_json(table_name="votes", file_path="../db_test/votes.json")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1

def test_insert_data_from_dataframe(set_up_db_conn):
    df = pd.DataFrame({"Id": [1,2], "Name": ["Adam", "Renata"]})
    set_up_db_conn._insert_data_from_dataframe(df=df, table_name="names")

    fetched_data = set_up_db_conn.engine.execute("select * from names order by Id desc").fetchall()
    assert len(fetched_data)==2
    assert fetched_data[1][1] == "Adam"
