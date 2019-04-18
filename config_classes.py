import luigi


class ImageTransferConfig(luigi.Config):
    password = luigi.Parameter()
    username = luigi.Parameter()
    machine = luigi.Parameter()
    options = luigi.Parameter()


class RockMakerDBConfig(luigi.Config):
    server = luigi.Parameter()
    database = luigi.Parameter()
    username = luigi.Parameter()
    password = luigi.Parameter()


class SentryConfig(luigi.Config):
    key = luigi.Parameter()
    ident = luigi.Parameter()