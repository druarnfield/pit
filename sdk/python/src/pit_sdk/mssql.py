from secrets import get_secret

from ibis import connect


def get_available_tables(connection_str_secret: str):
    con = connect(get_secret(connection_str_secret))
    return con.list_tables()
