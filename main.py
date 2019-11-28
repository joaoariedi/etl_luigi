# luigid --background --logdir ./logs
# python <this_file_name> <task_name>

import luigi
import requests
import json
import datetime


# Configuration classes
class OmieAPI(luigi.Config):
    key = luigi.Parameter()
    secret = luigi.Parameter()


# Tasks
class GetUserFromOmie(luigi.Task):
    date = luigi.DateSecondParameter(default=datetime.datetime.now())

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
    date = luigi.DateSecondParameter(default=datetime.datetime.now())

    def requires(self):
        return [GetUserFromOmie()]

    def get_name_id(self):
        with self.input()[0].open('r') as json_file:
            users = json.load(json_file)
        users_ids = []
        for cadastro in users['cadastros']:
            users_ids.append({'user_id': cadastro['nCodigo'],
                              'user_name': cadastro['cNome']})
        return users_ids

    def run(self):
        with self.output().open("w") as outfile:
            json.dump(self.get_name_id(), outfile, indent=4, ensure_ascii=False)

    def output(self):
        path = f"data/users_{str(self.date)}_ids.json"
        return luigi.LocalTarget(path)


if __name__ == '__main__':
    luigi.build([ExtractNameId()])
