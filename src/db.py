import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import logging

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


class DB:
    def __init__(self, db_path: str):
        self.db_path = f"sqlite:///{db_path}"
        self.engine = self._init_connection()

    def _init_connection(self):
        logging.info("connecting to db")
        return create_engine(self.db_path)

    def _existings_keys(self, table_name: str):
        try:
            keys = self.engine.execute(
                f"SELECT DISTINCT(Id) FROM {table_name};"
            ).fetchall()
        except OperationalError:
            return []

        keys_list = []
        for i in keys:
            for j in i:
                if j not in keys_list:
                    keys_list.append(j)
        logging.info(f"{len(keys_list)} rows in tabe found")
        return keys_list

    def _read_data_from_json(self, table_name: str, file_path: str) -> pd.DataFrame:
        # drop duplicated entries from original json file
        df = pd.read_json(file_path).drop_duplicates()
        existing_keys = self._existings_keys(table_name=table_name)
        return df.loc[lambda df: ~df["Id"].isin(existing_keys)]

    def _insert_data_from_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        logging.info(f"inserting {len(df)} rows for table {table_name}")
        df.to_sql(
            table_name,
            con=self.engine,
            if_exists="append",
            index_label="Id",
            index=False,
        )

    def create_or_refresh(self):
        # POSTS
        logging.info("create or refresh called for posts")
        tbl_posts = "posts"
        path_posts = "../db/raw/Posts.json"
        self._read_data_from_json(table_name=tbl_posts, file_path=path_posts).pipe(
            self._insert_data_from_dataframe, tbl_posts
        )

        # VOTES
        logging.info("create or refresh called for votes")
        tbl_votes = "votes"
        path_votes = "../db/raw/Votes.json"
        self._read_data_from_json(table_name=tbl_votes, file_path=path_votes).pipe(
            self._insert_data_from_dataframe, tbl_votes
        )
