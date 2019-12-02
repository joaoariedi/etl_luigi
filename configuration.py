import luigi


# Configuration classes
class OmieAPI(luigi.Config):
    key = luigi.Parameter()
    secret = luigi.Parameter()


class PostgresTable(luigi.Config):
    host = luigi.Parameter()
    password = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()