import json
from db import DB


config = {"db_path": "../db/db_posts_votes.sqlite"}


if __name__ == "__main__":

    db = DB(db_path=config["db_path"])
    db.create_or_refresh()
