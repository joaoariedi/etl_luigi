import luigi
from configuration import OmieAPI
import requests
import json


class GetVendedoresFromOmie(luigi.Task):
    date = luigi.DateSecondParameter()

    def run(self):
        key = OmieAPI().key
        secret = OmieAPI().secret

        headers = {
            "Content-type": "application/json"
        }

        data = {
            "call": "ListarVendedores",
            "app_key": key,
            "app_secret": secret,
            "param": [
                {
                    "pagina": 1,
                    "registros_por_pagina": 100,
                    "apenas_importado_api": "N",
                }
            ]
        }

        response = requests.post('https://app.omie.com.br/api/v1/geral/vendedores/', headers=headers, json=data)

        with self.output().open('w') as outfile:
            json.dump(response.json(), outfile, indent=4, ensure_ascii=False)

    def output(self):
        path = f"data/vendedores_{str(self.date)}.json"
        return luigi.LocalTarget(path)


class GetPedidosFromOmie(luigi.Task):
    date = luigi.DateSecondParameter()

    def run(self):
        key = OmieAPI().key
        secret = OmieAPI().secret

        headers = {
            "Content-type": "application/json"
        }

        data = {
            "call": "ListarPedidos",
            "app_key": key,
            "app_secret": secret,
            "param": [
                {
                    "pagina": 1,
                    "registros_por_pagina": 100,
                    "apenas_importado_api": "N",
                }
            ]
        }

        response = requests.post('https://app.omie.com.br/api/v1/produtos/pedido/', headers=headers, json=data)

        with self.output().open('w') as outfile:
            json.dump(response.json(), outfile, indent=4, ensure_ascii=False)

    def output(self):
        path = f"data/pedidos_{str(self.date)}.json"
        return luigi.LocalTarget(path)
