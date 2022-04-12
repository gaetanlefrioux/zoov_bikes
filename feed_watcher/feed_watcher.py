import json
import requests
from parser import free_bike_status_parser
import pandas as pd
import psycopg2
from io import StringIO

class FeedWatcher():
    def __init__(self, config_path):
        with open(config_path) as f:
            self.config = json.loads(f.read())
            self.conn = psycopg2.connect(
                host="pg_container",
                database="zoovbikes",
                user="postgres",
                password="pass"
            )
            print(f"Starting feed watcher with config:\n{json.dumps(self.config, indent=4)}")

    """
        Insert a dataframe into the given table
        params:
            df: [pandas dataframe] Data to insert
            table: [string] table where to insert
    """
    def insert_dataframe(self, df, table):
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cursor = self.conn.cursor()
        try:
            cursor.copy_from(buffer, table, sep=",", columns=df.columns.tolist())
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            cursor.close()
            raise error

        cursor.close()

    def run(self):
        r = requests.get(self.config["feed_uri"], params=self.config["payload"])
        data = r.json()
        timestamp = pd.to_datetime(data["last_updated"], unit="s")
        df = free_bike_status_parser.parse(data)

        df = df.rename(columns=self.config["name_mapping"])
        for col in self.config["timestamp_conversion"]:
            df[col] = pd.to_datetime(df[col], unit="s")

        print(f"Inserting {len(df)} free bikes for timestamp {timestamp}")
        self.insert_dataframe(df, self.config["table"])

        if "sql_function" in self.config and self.config["sql_function"]:
            cursor = self.conn.cursor()
            print(f"Calling SQL function {self.config['sql_function']}")
            cursor.callproc(self.config['sql_function'], [str(timestamp),])
            cursor.close()
            self.conn.commit()




if __name__ == '__main__':
    feed_watcher = FeedWatcher("./configs/free_bike_status_saclay.json")
    feed_watcher.run()
