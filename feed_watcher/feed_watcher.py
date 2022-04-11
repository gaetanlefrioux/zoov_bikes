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
            print(json.dumps(self.config, indent=4))

    def insert_dataframe(self, df, table):
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cursor = self.conn.cursor()
        try:
            cursor.copy_from(buffer, table, sep=",", columns=df.columns.tolist())
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self.conn.rollback()
            cursor.close()
            return 1

        cursor.close()

    def run(self):
        df_list = []
        for feed_uri in self.config["feed_uris"]:
            r = requests.get(feed_uri, params=self.config["payload"])
            data = r.json()
            df = free_bike_status_parser.parse(data)
            df_list += [df]

        df = pd.concat(df_list)
        print(len(df))
        df = df.rename(columns=self.config["name_mapping"])
        for col in self.config["timestamp_conversion"]:
            df[col] = pd.to_datetime(df[col], unit="s")

        self.insert_dataframe(df, self.config["table"])

if __name__ == '__main__':
    feed_watcher = FeedWatcher("./configs/free_bike_status.json")
    feed_watcher.run()
