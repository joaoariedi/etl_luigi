# luigid --background --logdir ./logs
# python <this_file_name> <task_name>

import luigi
import requests
import json
import datetime
import luigi.contrib.postgres
import pandas as pd
import csv


# Configuration classes
class OmieAPI(luigi.Config):
    key = luigi.Parameter()
    secret = luigi.Parameter()


class PostgresTable(luigi.Config):
    host = luigi.Parameter()
    password = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()


# Tasks
class GetUserFromOmie(luigi.Task):
    date = luigi.DateSecondParameter()

    def run(self):
        key = OmieAPI().key
        secret = OmieAPI().secret

        headers = {
            "Content-type": "application/json"
        }

        data = {
            "call": "ListarUsuarios",
            "app_key": key,
            "app_secret": secret,
            "param": [
                {
                    "pagina": 1,
                    "registros_por_pagina": 100
                }
            ]
        }

        response = requests.post('https://app.omie.com.br/api/v1/crm/usuarios/', headers=headers, json=data)

        with self.output().open('w') as outfile:
            json.dump(response.json(), outfile, indent=4, ensure_ascii=False)

    def output(self):
        path = f"data/users_{str(self.date)}.json"
        return luigi.LocalTarget(path)


class ExtractNameId(luigi.Task):
    date = luigi.DateSecondParameter()

    def requires(self):
        return [GetUserFromOmie(self.date)]

    def get_name_id(self):
        with self.input()[0].open('r') as json_file:
            users = json.load(json_file)
        users_ids = []
        for cadastro in users['cadastros']:
            users_ids.append({'code': cadastro['nCodigo'],
                              'name': cadastro['cNome']})
        return users_ids

    def run(self):
        with self.output().open("w") as outfile:
            df = pd.DataFrame(self.get_name_id())
            df.to_csv(outfile, encoding='utf-8', index=False, header=False)

    def output(self):
        path = f"data/users_{str(self.date)}_ids.csv"
        return luigi.LocalTarget(path)


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
        with self.input()[0].open('r') as input_file:
            reader = csv.reader(input_file)
            rows = [row for row in reader if len(row) == len(self.columns)]
            return rows

    def requires(self):
        return [ExtractNameId(self.date)]


class BuildTasks(luigi.Task):
    date = luigi.Parameter(default=datetime.datetime.now())

    def requires(self):
        return [WriteUserCodsToSQL(self.date)]


if __name__ == '__main__':
    luigi.build([BuildTasks()])
