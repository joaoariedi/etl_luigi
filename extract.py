import luigi
from configuration import OmieAPI
import requests
import json


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
