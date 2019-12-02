import luigi
from extract import GetVendedoresFromOmie, GetPedidosFromOmie
import json
import pandas as pd


class ExtractVendedorNameId(luigi.Task):
    date = luigi.DateSecondParameter()

    def requires(self):
        return [GetVendedoresFromOmie(self.date)]

    def get_name_id(self):
        with self.input()[0].open('r') as json_file:
            vendedores = json.load(json_file)
        vendedores_ids = []
        for cadastro in vendedores['cadastro']:
            vendedores_ids.append({'code': cadastro['codigo'],
                                   'name': cadastro['nome'],
                                   'ts': self.date})
        return vendedores_ids

    def run(self):
        df = pd.DataFrame(self.get_name_id())
        df.to_parquet(self.output().path, index=False)

    def output(self):
        path = f"data/vendedores_{str(self.date)}_ids.parquet"
        return luigi.LocalTarget(path)


class ExtractVendaId(luigi.Task):
    date = luigi.DateSecondParameter()

    def requires(self):
        return [GetPedidosFromOmie(self.date)]

    def get_name_id(self):
        with self.input()[0].open('r') as json_file:
            vendas = json.load(json_file)
        vendas_ids = []
        for venda in vendas['pedido_venda_produto']:
            vendas_ids.append({'code': int(venda['cabecalho']['codigo_pedido']),
                               'seller': int(venda['informacoes_adicionais']['codVend']),
                               'budget': int(venda['total_pedido']['valor_total_pedido']),
                               'ts': self.date})
        return vendas_ids

    def run(self):
        df = pd.DataFrame(self.get_name_id())
        df.to_parquet(self.output().path, index=False)

    def output(self):
        path = f"data/pedidos_{str(self.date)}_ids.parquet"
        return luigi.LocalTarget(path)