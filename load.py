import luigi
import pandas as pd
from configuration import PostgresTable
from transform import ExtractNameId
import luigi.contrib.postgres


class WriteUserCodsToSQL(luigi.contrib.postgres.CopyToTable):
    date = luigi.Parameter()
    host = PostgresTable().host
    password = PostgresTable().password
    database = PostgresTable().database
    user = PostgresTable().user
    table = 'sellers'

    columns = [
        ('code', 'bigint'),
        ('name', 'text'),
    ]

    def rows(self):
        df = pd.read_parquet(self.input()[0].path)
        rows = df.values.tolist()
        return rows

    def requires(self):
        return [ExtractNameId(self.date)]