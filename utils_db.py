# TimescaleDB database
class SetupDB:
    def __init__(self, db_host, db_port, db_user, db_pass, db_name, table_name, res, debug=False, verbose=True):
        import psycopg2

        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name
        self.table_name = table_name
        self.debug = debug
        self.res = res
        self.verbose = verbose
        self.conn = psycopg2.connect(host=self.db_host, port=self.db_port, user=self.db_user, password=self.db_pass, database=self.db_name)

    def __del__(self):
        self.conn.close()

    def get_start_times(self, devices, default_start, dynamic):
        """Get latest TimescaleDB timestamps for devices for use as 'start times' for listing log files from S3"""

        from datetime import datetime, timedelta
        from dateutil.tz import tzutc
        device_ids = [device.split("/")[1] for device in devices]
        start_times = []

        if dynamic == False:
            default_start_dt = datetime.strptime(default_start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tzutc())
            for device in device_ids:
                start_times.append(default_start_dt)
        else:
            for device in device_ids:
                last_time_query = f"SELECT time FROM {self.table_name} WHERE device_id = '{device}' ORDER BY time DESC LIMIT 1"
                last_time_result = self.conn.execute(last_time_query).fetchone()

                if last_time_result is None:
                    default_start_dt = datetime.strptime(default_start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tzutc())
                    last_time = default_start_dt
                else:
                    last_time = last_time_result[0] + timedelta(seconds=2)

                start_times.append(last_time)

                if self.verbose:
                    print(f"Log files will be fetched for {device} from {last_time}")

        return start_times

    def write_signals(self, device_id, df_phys):
        """Given a device ID and a dataframe of physical values,
        resample and write each signal to a time series database

        :param device_id:   ID of device (used as the 'measurement name')
        :param df_phys:     Dataframe of physical values (e.g. as per output of can_decoder)
        """
        tag_columns = []

        if df_phys.empty:
            print("Warning: Dataframe is empty, no data written")
            return
        else:
            if self.res != "":
                self.write_influx(device_id, df_phys, [])

            else:
                for signal, group in df_phys.groupby("Signal")["Physical Value"]:
                    df_signal = group.to_frame().rename(columns={"Physical Value": signal})

                    if self.res != "":
                        df_signal = df_signal.resample(self.res).ffill().dropna()

                    if self.verbose:
                        print(f"Signal: {signal} (mean: {round(df_signal[signal].mean(),2)} | records: {len(df_signal)} | resampling: {self.res})")

                    self.write_db(signal, df_signal)

    def write_db(self, name, df):
        """Helper function to write signal dataframes to TimescaleDB"""


        # df = df.rename(columns={
        #                "TimeStamp": "time",
        #                1: "value"
        #                })

        df["message"] = name

        print(df.head())

        print(df.columns.tolist())

        tmp_df = "./tmp_df.csv"
        df.to_csv(tmp_df, index_label='id', header=False, columns=["message", name])
        with open(tmp_df, 'r') as f:
            cursor = self.conn.cursor()
            cursor.copy_from(f, self.table_name, sep=',')

        self.conn.commit()

        # with self.conn.cursor() as cursor:
        #     for time, row in df.iterrows():
        #         cursor.execute(f"INSERT INTO {self.table_name} (time, message, value) VALUES ('{time}', '{name}', '{row[name]}')")
        #     self.conn.commit()

        if self.verbose:
            print(f"- SUCCESS: {len(df.index)} records of {name} written to TimescaleDB\n\n")
