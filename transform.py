import luigi
from extract import GetUserFromOmie
import json
import pandas as pd


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
        df = pd.DataFrame(self.get_name_id())
        df.to_parquet(self.output().path, index=False)

    def output(self):
        path = f"data/users_{str(self.date)}_ids.parquet"
        return luigi.LocalTarget(path)