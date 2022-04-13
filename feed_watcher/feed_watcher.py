import json
import requests
import pandas as pd
import psycopg2
from io import StringIO
from time import perf_counter, sleep
import argparse
import importlib.util

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
        parser_spec = importlib.util.spec_from_file_location("parser", f"./parser/{self.config['parser']}.py")
        parser = importlib.util.module_from_spec(parser_spec)
        parser_spec.loader.exec_module(parser)
        self.parser = parser

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

    """
        Ingest data into the database
        params:
            data: [dict] data coming from the feed in json format
    """
    def ingest_data(self, data):
        df = self.parser.parse(data)

        df = df.rename(columns=self.config["name_mapping"])
        for col in self.config["timestamp_conversion"]:
            df[col] = pd.to_datetime(df[col], unit="s")

        print(f"Inserting {len(df)} free bikes for timestamp {self.current_sys_time}")
        self.insert_dataframe(df, self.config["table"])

        if "sql_function" in self.config and self.config["sql_function"]:
            cursor = self.conn.cursor()
            print(f"Calling SQL function {self.config['sql_function']}")
            cursor.callproc(self.config['sql_function'], [str(self.current_sys_time),])
            cursor.close()
            self.conn.commit()

    def run(self):
        self.last_updated = None
        self.ttl = None
        while True:
            r = requests.get(self.config["feed_uri"], params=self.config["payload"])
            data = r.json()

            self.ttl = data["ttl"]
            self.current_sys_time = pd.to_datetime(data["last_updated"], unit="s")

            if self.last_updated == None or self.last_updated < self.current_sys_time:
                t_start = perf_counter()
                self.ingest_data(data)
                self.last_updated = self.current_sys_time
                t_stop = perf_counter()
                print(f"Data ingested for timestamp {self.last_updated} in {(t_stop-t_start):.3f}s")
                sleep_time = (self.ttl/3) - (t_stop-t_start)
            else:
                print(f"Data already processed for time {self.current_sys_time}")
                sleep_time = self.ttl/3

            print(f"Sleeping for {sleep_time:.3f}s")
            sleep(sleep_time)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Path to the desired config file", required=True)

    args = parser.parse_args()
    feed_watcher = FeedWatcher(f"./configs/{args.config}")
    feed_watcher.run()
