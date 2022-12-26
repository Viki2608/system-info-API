import connexion
from typing import Tuple
from http import HTTPStatus

from swagger_server.util import util
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

from swagger_server.util.system_info_utils import *


def systeminfo_webhook():  # noqa: E501
    print('Received request from systeminfo_webhook')
    try:
        if not connexion.request.is_json:
            message = 'Content type is not json response from the webhook!!'
            print(message)
            return message, HTTPStatus.UNSUPPORTED_MEDIA_TYPE
        data = connexion.request.get_json()
        if data is None or (type(data) is not dict):
            message = 'Data received is not of type dict'
            print(message)
            return message, HTTPStatus.NOT_ACCEPTABLE
        return processing_webhook(data)
    except Exception as ex:
        message = 'Error while processing the system info webhook: {}'.format(str(ex))
        print(message)
        return message, HTTPStatus.INTERNAL_SERVER_ERROR

def processing_webhook(data: dict) -> Tuple[str, int]:

    print('Processing the data from the gcm webhook')
    try:
        db_url = "postgresql://postgres:mysecretpassword@localhost:5432/systeminfo"
        print(db_url)
        if not database_exists(db_url):
            create_database(db_url)
        engine = create_engine(db_url)
        print(engine)
        Base.metadata.create_all(engine)
        httpstatus = persisting_system_info(data,engine)
        if httpstatus == 200:
            return "System information inserted successfully",HTTPStatus.OK
        
        else:
            return "System information insertion failed",HTTPStatus.INTERNAL_SERVER_ERROR
    except Exception as ex:
        print(str(ex))
        return str(ex), HTTPStatus.INTERNAL_SERVER_ERROR