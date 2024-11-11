# ==================================================================================
#       Copyright (c) 2020 AT&T Intellectual Property.
#       Copyright (c) 2020 HCL Technologies Limited.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# ==================================================================================
import os   # new
import json # new  
import datetime # new
from influxdb_client import InfluxDBClient, Point # new
from influxdb_client.client.write_api import SYNCHRONOUS # new
# from influxdb import DataFrameClient
from configparser import ConfigParser
from mdclogpy import Logger
from src.exceptions import NoDataError
from influxdb_client.client.exceptions import InfluxDBError # new
# from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
from requests.exceptions import RequestException
import pandas as pd
import time

logger = Logger(name=__name__)


class DATABASE(object):

    def __init__(self, bucket="kpimon", token='MYTOKE', username='admin', password='123', influxDBAdress="r4-influxdb-influxdb2.ricplt", organization='8086', ssl=False):
        self.token = token
        self.username = username
        self.password = password
        self.influxDBAdress = influxDBAdress
        self.organization = organization
        self.bucket = bucket
        self.ssl = ssl
        self.data = None
        self.client = None
        self.config()

    def connect(self):
        if self.client is not None:
            self.client.close()

        try:
            self.client = InfluxDBClient(url=self.influxDBAdress, token=self.token, verify_ssl=self.ssl, org=self.organization)
            version = self.client.version()
            logger.info("Conected to Influx Database, InfluxDB version : {}".format(version))
            return True

        except (RequestException, InfluxDBError, ConnectionError):
            logger.error("Failed to establish a new connection with InflulxDB, Please check your url/hostname")
            time.sleep(120)

    def read_data(self, meas='ueMeasReport', limit=10000, cellid=False, ueid=False):
        if cellid:
            meas = self.cellmeas
            param = self.cid
            Id = cellid
            query = """from(bucket:"{}") |> range(start: -1h) |> filter(fn: (r) => """.format(self.bucket)
            query += """r._measurement == "{}" and """.format(meas)
            query += """r.{} == "{}" and (r._field == "{}" or r._field == "{}")) """.format(param, Id, self.thptparam[0], self.thptparam[1])
            query += """|> group() """
            query += """|> sort(columns: ["_time"], desc: true) |> limit(n: {}*2)""".format(limit)
            query += """|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")"""

        if ueid:
            meas = self.uemeas
            param = self.ue
            limit = 1
            Id = ueid
            query = """from(bucket:"{}") |> range(start: -2h) |> filter(fn: (r) => """.format(self.bucket)
            query += """r._measurement == "{}" and """.format(meas)
            query += """r.{} == "{}" ) """.format(param,Id)
            query += """|> group() """
            query += """|> sort(columns: ["_time"], desc: true) |> limit(n: {})""".format(limit)
        
        logger.info("Query Command: {}".format(query))
        self.query(query, meas, Id)

    def query(self, query, meas, Id=False):
        try:
            queryapi = self.client.query_api()
            result = queryapi.query_data_frame(query)
            if len(result) == 0:
                raise NoDataError
            else:
                self.data = result

        except (RequestException, InfluxDBError):
            logger.error("Failed to connect to influxdb")

        except NoDataError:
            self.data = None
            if Id:
                logger.error("Data not found for " + Id + " in measurement: "+meas)
            else:
                logger.error("Data not found for " + meas)

    def cells(self, meas='CellReports', limit=100):
        meas = self.cellmeas
        query = """select * from {}""".format(meas)
        query += " ORDER BY DESC LIMIT {}".format(limit)
        self.query(query, meas)
        if self.data is not None:
            return self.data[self.cid].unique()

    def write_prediction(self, df, bucket='qp'):
        try:
            # print(type(df.index))
            # print(df['Viavi_GnbDuId'].tolist()[0])
            row = df.index.tolist()
            ts = row[0].strftime('%Y-%m-%d %X')

            writeapi = self.client.write_api(write_options=SYNCHRONOUS)
            writeapi.write(bucket, self.organization, Point("PredictThp").tag("Viavi_GnbDuId", df['Viavi_GnbDuId'].tolist()[0]).tag("DRB_UEThpUl", df['DRB_UEThpUl'].tolist()[0]).tag("DRB_UEThpDl", df['DRB_UEThpDl'].tolist()[0]).field("index", ts))
        except (RequestException, InfluxDBError):
            logger.error("Failed to send metrics to influxdb")

    def config(self):
        config_file_path = os.environ.get("CONFIG_FILE", None)
        with open(config_file_path) as config_file:
            config = json.load(config_file)
            DB_config  = config['influxDB']
            self.influxDBAdres = DB_config['influxDBAdress']
            self.username = DB_config['username']
            self.password = DB_config['password']
            self.token = DB_config['token']
            self.organization = DB_config['organization']
            self.bucket = DB_config['bucket']
        logger.info("influxDBAdress: {}, username: {}, password: {}, organization: {}, token: {}, bucket: {}.".format(self.influxDBAdres, self.username, self.password, self.organization, self.token, self.bucket))

        cfg = ConfigParser()
        cfg.read('src/qp_config.ini')
        for section in cfg.sections():
            if section == 'influxdb':
                # self.host = cfg.get(section, "host")
                # self.port = cfg.get(section, "port")
                # self.user = cfg.get(section, "user")
                # self.password = cfg.get(section, "password")
                # self.path = cfg.get(section, "path")
                self.ssl = cfg.get(section, "ssl")
                # self.dbname = cfg.get(section, "database")
                self.cellmeas = cfg.get(section, "cellmeas")
                self.uemeas = cfg.get(section, "uemeas")

            if section == 'features':
                self.thptparam = [cfg.get(section, "thptUL"), cfg.get(section, "thptDL")]
                self.nbcells = cfg.get(section, "nbcells")
                self.servcell = cfg.get(section, "servcell")
                self.ue = cfg.get(section, "ue")
                self.cid = cfg.get(section, "cid")


class DUMMY(DATABASE):

    def __init__(self):
        super().__init__()
        self.ue_data = pd.DataFrame([[1002, "c2/B13", 8, 69, 65, 113, 0.1, 0.1, "Car-1", -882, -959, pd.to_datetime("2021-05-12T07:43:51.652")]], columns=["du-id", "RF.serving.Id", "prb_usage", "rsrp", "rsrq", "rssinr", "throughput", "targetTput", "ue-id", "x", "y", "measTimeStampRf"])

        self.cell = pd.read_csv('src/cells.csv')

    def read_data(self, meas='ueMeasReport', limit=100000, cellid=False, ueid=False):
        if ueid:
            self.data = self.ue_data.head(limit)
        if cellid:
            self.data = self.cell.head(limit)

    def cells(self):
        return self.cell[self.cid].unique()

    def write_prediction(self, df, meas_name='QP'):
        pass

    def query(self, query=None):
        return {'UEReports': self.ue_data.head(1)}
