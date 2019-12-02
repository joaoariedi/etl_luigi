import pandas as pd
from configuration import PostgresTable
from transform import ExtractVendedorNameId, ExtractVendaId
import luigi.contrib.postgres


class WriteSellersCodsToSQL(luigi.contrib.postgres.CopyToTable):
    date = luigi.DateSecondParameter()
    host = PostgresTable().host
    password = PostgresTable().password
    database = PostgresTable().database
    user = PostgresTable().user

    table = 'sellers'
    columns = [
        ('code', 'bigint'),
        ('name', 'text'),
        ('ts', 'timestamp'),
    ]

    def rows(self):
        df = pd.read_parquet(self.input()[0].path)
        rows = df.values.tolist()
        return rows

    def requires(self):
        return [ExtractVendedorNameId(self.date)]


class WriteOrdersCodsToSQL(luigi.contrib.postgres.CopyToTable):
    date = luigi.DateSecondParameter()
    host = PostgresTable().host
    password = PostgresTable().password
    database = PostgresTable().database
    user = PostgresTable().user

    table = 'orders'
    columns = [
        ('code', 'bigint'),
        ('seller', 'bigint'),
        ('budget', 'bigint'),
        ('ts', 'timestamp'),
    ]

    def rows(self):
        df = pd.read_parquet(self.input()[0].path)
        rows = df.values.tolist()
        return rows

    def requires(self):
        return [ExtractVendaId(self.date)]