import logging
from typing import Tuple
from http import HTTPStatus

import csv
import pandas as pd

from flask import request

from swagger_server.utils.get_users_info import get_postgres_user_info
from swagger_server.utils.persist_to_databases import persist_playbook_metrics_postgres
from swagger_server.utils.persist_to_databases import persist_playbook_metrics_trends_postgres

log = logging.getLogger(__name__)

def playbook_enforcement_webhook() -> Tuple[str, int]:
    """
    This function will be called when HTTP POST request is sent to /playbook_enforcement
    :return: message and status code
    """
    print('Received request from playbook_enforcement_webhook')
    try:
        csv_file = request.files['file']
        if csv_file.filename == '':
            return "No selected file", HTTPStatus.BAD_REQUEST
        if not csv_file:
            return "No file", HTTPStatus.BAD_REQUEST
        if not csv_file.filename.endswith('.csv'):
            return "Invalid file format", HTTPStatus.BAD_REQUEST

        return process_webhook(csv_file)
    except Exception as ex:
        message = 'Error while processing the playbook_enforcement webhook: {}'.format(str(ex))
        log.exception(message)
        return message, HTTPStatus.INTERNAL_SERVER_ERROR


def process_webhook(csv_file: csv) -> Tuple[str, int]:
    """
    This function is to process the payload from playbook enforcement webhook
    :param file: csv file received from the webhook
    :return: message and status code
    """

    try:
        # getting postgres user info
        postgres_server, postgres_user, postgres_token, postgres_db = get_postgres_user_info()

        try:
            df = pd.read_csv(csv_file, sep='|', skiprows=1)
        except Exception as ex:
            message = 'Error while Reading csv file: {}'.format(str(ex))
            log.exception(message)
            return message, HTTPStatus.INTERNAL_SERVER_ERROR
        df.fillna('NULL', inplace=True)
        playbook_enforcement_dict = df.to_dict('records')
        message, playbook_metrics_status_code = persist_playbook_metrics_postgres(
                                                playbook_enforcement_dict,
                                                postgres_server,
                                                postgres_user,
                                                postgres_token,
                                                postgres_db)
        message, playbook_trends_status_code = persist_playbook_metrics_trends_postgres(
                                                postgres_server,
                                                postgres_user,
                                                postgres_token,
                                                postgres_db)

        if playbook_metrics_status_code == HTTPStatus.OK and \
            playbook_trends_status_code == HTTPStatus.OK:
            message = 'Sucessfully Persisted Ansible Enforcement Metrics data'
            return message, HTTPStatus.OK
        else:
            message = 'Failed to Persist Ansible Enforcement Metrics data'
            return message, HTTPStatus.INTERNAL_SERVER_ERROR
    except Exception as ex:
        message = 'Error inserting Playbook Enforcement metrics: {}'.format(str(ex))
        log.exception(message)
        return message, HTTPStatus.INTERNAL_SERVER_ERROR
