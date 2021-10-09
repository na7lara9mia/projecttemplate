import cx_Oracle
import sqlalchemy as sa


def create_db_uri(db_config, dialect=None):
    """ Create the correct uri for a specific database configuration.

    Parameters
    ----------
    db_config: dict
        parameters for the use of the database (type, host, port, service_name, username, password)
    dialect: str
        name of the specific connector to use (None by default)

    Returns
    -------
    uri: str
        uri to access the database
    """
    if dialect is None:
        if db_config['type'] == 'oracle':
            return create_db_uri(db_config, 'oracle+cx_oracle')
        else:
            raise NotImplementedError('You must specify a valid "type" in config')
    if dialect == 'oracle+cx_oracle':
        dns = cx_Oracle.makedsn(
            host=db_config['host'],
            port=db_config['port'],
            service_name=db_config['service_name'])
        uri = "{}://{}:{}@{}".format(
            dialect,
            db_config['username'],
            db_config['password'],
            dns)
    elif dialect.startswith('jdbc'):
        uri = "{}:{}/{}@//{}:{}/{}".format(
            dialect,
            db_config['username'],
            db_config['password'],
            db_config['host'],
            db_config['port'],
            db_config['service_name'],
        )
    else:
        raise NotImplementedError('Can only connect when db_config["type"] is "oracle"')
    return uri


def connect(uri):
    """returns a sqlAlchemy.connection object to the default database"""
    engine = sa.create_engine(uri)
    return engine.connect()
