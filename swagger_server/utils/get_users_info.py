import os
import re
import logging
from typing import Tuple

import yaml
import requests
from flask import current_app as app

from swagger_server import constants
# from swagger_server.exceptions import MetricsError

log = logging.getLogger(__name__)


def get_token_from_the_vault(vkey: str) -> str:
    """
    This function is to get a token from the vault
    :param vkey: vault key name
    :return: token
    """
    log.debug('Getting token from the vault: {}'.format(vkey))

    token = None
    try:
        vault_obj = app.config.get('vault_obj')
        token = vault_obj.get_secret_value(vkey)
    except Exception as ex:
        message = 'Error while getting token from the vault: {}'.format(str(ex))
        log.exception(message)
        # raise MetricsError(message)

    return token


def get_github_token() -> str:
    """
    This function is to get the github token from the vault
    :param: None
    :return: github token
    """
    log.debug('Getting github token')
    github_token = None

    try:
        github_vkey = app.config.get('github_vkey')
        github_token = get_token_from_the_vault(github_vkey)
    except Exception as ex:
        message = 'Error while getting github token: {}'.format(str(ex))
        log.exception(message)
        # raise MetricsError(message)

    return github_token


def load_jenkins_mapper() -> dict:
    """
    This function is to load the jenkins mapper from a config file
    :param: None
    :return: jenkins mapper data
    """
    log.info('Loading jenkins mapper file')
    git_token = get_github_token()
    jenkins_mapper = dict()

    try:
        redirects_count = 0
        branch = os.environ.get(constants.ENV_NAME)

        with requests.Session() as sess:
            sess.headers.update({'Authorization': constants.TOKEN.format(git_token),
                                 'Accept': 'application/vnd.github.VERSION.raw'})

            content_url = constants.CONTENT_URL.format(constants.DIGITAL_DEVOPS_ORG, constants.DYNAFLO_STORE,
                                                       constants.PIPELINE_INFO_YML, branch)
            while True:
                # authentication headers are being dropped while automatic redirection in requests library
                # hence redirects are disabled and handled properly
                log.debug('Content URL: {}'.format(content_url))
                resp = sess.get(content_url, allow_redirects=False)
                status_code = resp.status_code

                if status_code in constants.REDIRECT_STATUS_CODES:
                    redirects_count += 1
                    if redirects_count > constants.MAX_REDIRECTS:
                        raise requests.exceptions.TooManyRedirects('Excceeded max redirects for content url')

                    # redirection url
                    content_url = resp.headers['Location']
                elif status_code == 200:
                    pipeline_info = resp.text
                    pipeline_yaml = yaml.safe_load(pipeline_info)
                    jenkins = pipeline_yaml.get('pipeline_info').get('jenkins').get('infra')
                    for key, row in jenkins.items():
                        url = row.get('url').strip('/')
                        jenkins_mapper[url] = {'user': row.get('user'), 'token': row.get('vkey')}
                    break
                else:
                    message = '{}: {}'.format(status_code, resp.text)
                    log.error(message)
                    # raise MetricsError(message)
    except Exception as ex:
        message = 'Error while loading jenkins mapper file: {}'.format(str(ex))
        log.exception(message)
        # raise MetricsError(message)

    log.debug('Jenkins mapper: {}'.format(jenkins_mapper))
    return jenkins_mapper


def get_jenkins_user_info_from_build_url(job_url: str) -> Tuple[str, str, str]:
    """
    This function is to get jenkins user info from job url
    :param job_url: jenkins job url
    :return: jenkins server url, username, and token
    """
    log.debug('Getting jenkins user info')
    jenkins_server = None
    jenkins_user = None
    jenkins_token = None

    try:
        jenkins_mapper = app.config.get('jenkins_mapper')
        # get jenkins server url from job url
        match = re.search(constants.BUILD_URL_REGEX, job_url)
        if match:
            jenkins_server = match.group(1)
        else:
            message = 'Unable to parse ienkins build url to get ienkins user info'
            log.error(message)
            # raise MetricsError(message)

        jenkins_user = jenkins_mapper.get(jenkins_server).get('user')
        jenkins_vkey = jenkins_mapper.get(jenkins_server).get('token')
        jenkins_token = get_token_from_the_vault(jenkins_vkey)
    except Exception as ex:
        message = 'Error while getting jenkins user info from build url: {}'.format(str(ex))
        log.exception(message)
        # raise MetricsError(message)

    return jenkins_server, jenkins_user, jenkins_token


def get_jenkins_user_info(jenkins_server) -> Tuple[str, str, str]:
    """
    This function is to get jenkins user info from the app config
    :param jenkins_server: jenkins server url
    :return: jenkins server url, username, and token
    """
    log.debug('Getting jenkins user info')
    jenkins_user = None
    jenkins_token = None

    try:
        jenkins_mapper = app.config.get('jenkins_mapper')
        jenkins_user = jenkins_mapper.get(jenkins_server).get('user')
        jenkins_vkey = jenkins_mapper.get(jenkins_server).get('token')
        jenkins_token = get_token_from_the_vault(jenkins_vkey)
    except Exception as ex:
        message = 'Error while getting jenkins user info: {}'.format(str(ex))
        log.exception(message)
        # raise MetricsError(message)

    return jenkins_server, jenkins_user, jenkins_token

def get_sonarqube_user_info(sonar_url: str) -> Tuple[str, str]:
    """
    This function is to get the sonar info from the app config
    :param: sonar_url
    :return: sonar server url, sonar username, and token
    """
    log.debug('Getting sonarqube user info')
    sonar_user = None
    sonar_token = None

    try:
        if sonar_url:
            split_url = sonar_url.split('/')
            sonar_server = '//'.join([split_url[0], split_url[2]])
        else:
            # assuming that the default is NA sonarqube server
            sonar_server = constants.NA_SONAR_SERVER
        config = app.config.get('metrics_yml').get('sonarqube')

        if sonar_server in config:
            sonar_user = config.get(sonar_server).get('sonar_user')
            sonar_vkey = config.get(sonar_server).get('sonar_vkey')
            sonar_token = get_token_from_the_vault(sonar_vkey)
    except Exception as ex:
        message = 'Error while getting sonar user info: {}'.format(str(ex))
        log.exception(message)
        # raise MetricsError(message)

    return sonar_server, sonar_user, sonar_token


def get_postgres_user_info() -> Tuple[str, str, str, str]:
    """
    This function is to get the postgres info from the app config
    :param: None
    :return: postgres url, username, token, and database name
    """
    log.debug('Getting postgres user info')
    postgres_server = None
    postgres_user = None
    postgres_token = None
    postgres_db = None

    try:
        print("Getting postgres user info")
        # postgres_server = app.config.get('postgres_ip')
        # postgres_user = app.config.get('postgres_user')
        # postgres_vkey = app.config.get('postgres_vkey')
        # postgres_token = get_token_from_the_vault(postgres_vkey)
        # postgres_db = app.config.get('postgres_db')
    except Exception as ex:
        message = 'Error while getting postgres user info: {}'.format(str(ex))
        log.exception(message)
        # raise MetricsError(message)

    return "localhost", "postgres", "mysecretpassword", "postgres"


def get_grafana_user_info() -> Tuple[str, str, str]:
    """
    This function is to get the grafana info from the app config
    :param: None
    :return: grafana url, username, and token
    """
    log.debug('Getting grafana user info')
    grafana_server = None
    grafana_user = None
    grafana_token = None

    try:
        grafana_server = app.config.get('grafana_url')
        grafana_user = app.config.get('grafana_user')
        grafana_vkey = app.config.get('grafana_token')
        grafana_token = get_token_from_the_vault(grafana_vkey)
    except Exception as ex:
        message = 'Error while getting grafana user info: {}'.format(str(ex))
        log.exception(message)
        # raise MetricsError(message)

    return grafana_server, grafana_user, grafana_token


def get_jira_user_info(jira_server) -> Tuple[str, str, str]:
    """
    This function is to get the jira user infor from the app config
    param jira_server: Jira server url
    return: jira server url, username, and token
    """
    log.debug('Getting jira user info')
    jira_user = None
    jira_token = None

    try:
        jira_mapper = app.config.get('jira_mapper')
        jira_user = jira_mapper.get(jira_server).get('user')
        jira_vkey = jira_mapper.get(jira_server).get('token')
        jira_token = get_token_from_the_vault(jira_vkey)
    except Exception as ex:
        message = 'Error while getting jira user info: {}'.format(str(ex))
        log.exception(message)
        # raise MetricsError(message)

    return jira_server, jira_user, jira_token
