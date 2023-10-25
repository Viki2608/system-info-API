import logging
import psycopg2
from contextlib import contextmanager
# from swagger_server.exceptions import MetricsError

log = logging.getLogger(__name__)


@contextmanager
def connect_to_database(postgres_server: str, postgres_user: str, postgres_token: str, postgres_db: str):
    """
    This function is to create a database connection object
    :param postgres_server: postgres server address
    :param postgres_user: postgres username
    :param postgres_token: postgres password
    :param postgres_db: postgres database name
    :return: connection and cursor objects
    """
    try:
        conn = psycopg2.connect(host="localhost", user="postgres", password="mysecretpassword",
                                dbname="postgres", connect_timeout=5)
        cursor = conn.cursor()
        print('Connected to the database')
        yield conn, cursor
        cursor.close()
        conn.close()
        print('Database connection closed')
    except Exception as ex:
        message = 'Database error: {}'.format(str(ex))
        log.exception(message)
        # raise MetricsError(message)


def get_select_query(cursor, table_name: str, select_data: list, where_data={}, groupby_data = []) -> str:
    """
    This function is to construct a select query for a given table
    :param cursor: database cursor object
    :param table_name: table where data to be fetched from
    :param select_data: data to be fetched from a table
    :param where_data: data to be in where clause
    :param groupby_data: dataa to be in group by clause
    :return: constructed select query
    """
    select_clause = ', '.join(select_data)

    where_clause = ''
    where_keys = list()
    if where_data:
        temp = list()
        for key, value in where_data.items():
            where_keys.append(value)
            if value is None:
                temp.append('{} is %s'.format(key))
            else:
                temp.append('{}=%s'.format(key))
        where_clause = ' WHERE {}'.format(' AND '.join(temp))

    groupby_clause = ''
    if groupby_data:
        groupby_clause = ' GROUP BY {}'.format(', '.join(groupby_data))

    prepared_query = 'SELECT {} FROM {}{}{};'.format(select_clause, table_name, where_clause, groupby_clause)
    select_query = cursor.mogrify(prepared_query, where_keys)

    print('Select query: {}'.format(select_query))
    return select_query


def get_insert_query(cursor, table_name: str, insert_data: list) -> str:
    """
    This function is to construct an insert query for a given table
    :param cursor: database cursor object
    :param table_name: table where data to be inserted
    :param insert_data: data to be inserted into a table
    :return: constructed insert query
    """
    if type(insert_data) == list:
        prepared_query = 'INSERT INTO {} VALUES ({});'.format(table_name, ', '.join(['%s'] * len(insert_data)))
        insert_query = cursor.mogrify(prepared_query, insert_data)
    elif type(insert_data) == dict:
        prepared_query = 'INSERT INTO {} ({}) VALUES ({});'.format(table_name, ', '.join(insert_data.keys()),
                                                                   ', '.join(['%s'] * len(insert_data)))
        insert_query = cursor.mogrify(prepared_query, list(insert_data.values()))

    print('Insert query: {}'.format(insert_query))
    return insert_query


def get_update_query(cursor, table_name: str, update_data: dict, where_data: dict) -> str:
    """
    This function is to construct an update query for a given table
    :param cursor: database cursor object
    :param table_name: table where data to be updated
    :param update_data: data to be updated
    :param where_data: data to be in where clause
    :return: constructed update query
    """
    update_clause = ''
    update_keys = list()
    if update_data:
        temp = list()
        for key, value in update_data.items():
            update_keys.append(value)
            temp.append('{}=%s'.format(key))
        update_clause = ', '.join(temp)

    where_clause = ''
    where_keys = list()
    temp = list()
    for key, value in where_data.items():
        where_keys.append(value)
        if value is None:
            temp.append('{} is %s'.format(key))
        else:
            temp.append('{}=%s'.format(key))
    where_clause = ' AND '.join(temp)

    prepared_query = 'UPDATE {} SET {} WHERE {};'.format(table_name, update_clause, where_clause)
    update_query = cursor.mogrify(prepared_query, update_keys + where_keys)

    print('Update query: {}'.format(update_query))
    return update_query


def get_delete_query(cursor, table_name: str, where_data: dict) -> str:
    """
    This function to construct a delete query for a given table
    :param cursor: cursor object
    :param table_name: table name from which data to be deleted
    :param where_data: where clause conditions
    :return: constructed delete query
    """
    where_clause = ''
    where_keys = list()
    temp = list()
    for key, value in where_data.items():
        where_keys.append(value)
        if value is None:
            temp.append('{} is %s'.format(key))
        else:
            temp.append('{}=%s'.format(key))
    where_clause = ' AND '.join(temp)

    prepared_query = 'DELETE FROM {} WHERE {};'.format(table_name, where_clause)
    delete_query = cursor.mogrify(prepared_query, where_keys)

    print('Delete query: {}'.format(delete_query))
    return delete_query
