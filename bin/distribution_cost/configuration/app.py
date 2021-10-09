import os
import logging
# from functools import wraps
import yaml

from ..infra import db_connection

app_name = 'distribution_cost'
REPO_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__),
    '..',
    '..',
    '..',
))


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class AppConfig(metaclass=Singleton):
    """Holds all application configuration: credentials, logging, preprocessing."""
    # config filepaths
    config_store_path = os.path.join(REPO_PATH, 'conf')
    _logging_conf_filepath = os.path.join(config_store_path, 'logging.conf.yml')
    _application_conf_filepath = os.path.join(config_store_path, 'application.yml')
    print(_application_conf_filepath)
    print(REPO_PATH)

    def __init__(self, app_name=app_name):
        self._prepare_logging()
        logger = logging.getLogger()
        logger.info("============================================================")
        logger.info("============================================================")
        logger.info("  AppConfig in '{}'".format(self._application_conf_filepath))
        try:
            with open(self._application_conf_filepath) as f:
                self._application_config = yaml.load(f.read(), Loader=yaml.FullLoader)
                self._prepare_brcdb_configuration()
                self._check_jdbcdb_connection()
                self._check_cx_oracledb_connection()
                self._prepare_mailing_configuration()
        except FileExistsError:
            pass

    def _prepare_logging(self):
        """
        Logging configuration.

        First part is a classic console + file logging. The file is a timebased
        rotating file. All this is configured in config-store/logging.conf.yml.

        Second part is the SentryHandler. Because of the credentials used to log
        to the appropriate Sentry Project, this part requires an 'application.yml'
        file. Normally, it is a symlink to one of the actual conffiles:

        - development.yml
        - preproduction.yml
        - production.yml

        Those files all have different credentials.
        """
        # create log dir
        try:
            dirname = 'log'  # as specified in logging.conf.yml
            if not os.path.exists(dirname):
                os.makedirs(dirname)
        except FileExistsError:
            pass
        import logging.config
        with open(self._logging_conf_filepath) as f:
            dict_conf = yaml.load(f.read(), Loader=yaml.FullLoader)
            logging.config.dictConfig(dict_conf)

    @property
    def brcdb_conn_dict(self):
        """Return BrcDB connection dict."""
        return self._brcdb_conn_dict

    @property
    def db_uri_jdbc(self):
        """Database uri connection."""
        return self.db_uri_conn

    @property
    def db_uri_cx_oracle(self):
        """Database uri connection."""
        return self.db_uri_cx_oracle_conn

    def _prepare_brcdb_configuration(self):
        logger = logging.getLogger()
        logger.info("  - DB:  read brc db configuration.")
        conn_dict = {}

        config = self._application_config
        if 'brcdb' not in config:
            raise(KeyError("BrcDB configuration not found."))
        dbnode = config['brcdb']
        if 'host' not in dbnode:
            raise(KeyError("missing host definition in db configuration"))
        if 'short_name' not in dbnode:
            raise(KeyError("missing short_name definition in db configuration"))
        if 'service_name' not in dbnode:
            raise(KeyError("missing service_name definition in db configuration"))
        if 'username' not in dbnode:
            raise(KeyError("missing username definition in db configuration"))
        if 'password' not in dbnode:
            raise(KeyError("missing password definition in db configuration"))
        if 'port' not in dbnode:
            raise(KeyError("missing port definition in db configuration"))
        conn_dict['host'] = dbnode['host']
        conn_dict['short_name'] = dbnode['short_name']
        conn_dict['service_name'] = dbnode['service_name']
        conn_dict['username'] = dbnode['username']
        conn_dict['password'] = dbnode['password']
        conn_dict['port'] = dbnode['port']

        self._brcdb_conn_dict = conn_dict

    def _check_jdbcdb_connection(self):
        logger = logging.getLogger()
        logger.info("  - DB:  check exadata_brc db connection.")
        db_uri = db_connection.create_db_uri(self._brcdb_conn_dict,
                                             dialect='jdbc:oracle:thin')
        self.db_uri_conn = db_uri

    def _check_cx_oracledb_connection(self):
        logger = logging.getLogger()
        logger.info("  - DB:  check exadata_brc db connection.")
        db_uri_cx_oracle = db_connection.create_db_uri(self._brcdb_conn_dict,
                                                       dialect='oracle+cx_oracle')
        self.db_uri_cx_oracle_conn = db_uri_cx_oracle

    def _prepare_mailing_configuration(self):
        logger = logging.getLogger()
        logger.info("  - DB:  read mailing configuration.")
        conn_dict = {}

        config = self._application_config
        if 'mailing' not in config:
            raise(KeyError("Mailing configuration not found."))
        dbnode = config['mailing']
        if 'host' not in dbnode:
            raise(KeyError("missing host definition in db configuration"))
        if 'sender' not in dbnode:
            raise(KeyError("missing sender definition in db configuration"))
        if 'receivers' not in dbnode:
            raise(KeyError("missing receivers definition in db configuration"))
        if 'username' not in dbnode:
            raise(KeyError("missing username definition in db configuration"))
        if 'password' not in dbnode:
            raise(KeyError("missing password definition in db configuration"))
        conn_dict['host'] = dbnode['host']
        conn_dict['sender'] = dbnode['sender']
        conn_dict['receivers'] = dbnode['receivers']
        conn_dict['user'] = dbnode['username']
        conn_dict['passwd'] = dbnode['password']

        self._mailing_conn_dict = conn_dict
