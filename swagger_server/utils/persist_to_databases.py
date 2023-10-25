import re
import logging
from typing import Tuple
from http import HTTPStatus
from datetime import datetime
from datetime import timezone

from swagger_server import constants
from swagger_server.utils import postgres_utils
# from swagger_server.exceptions import MetricsError

log = logging.getLogger(__name__)

# commands to create tables for the below functions are in this path:
# swagger_server\test\sample_inputs\postgres_tables.txt
def persist_dynaflo_requests_into_postgres(requests_data: dict, postgres_server: str, postgres_user: str,
                                             postgres_token: str, postgres_db: str):
    """
    This function is to persist dynaflo request data into postgres db
    :param requests_data: dynaflo requests data
    :param postgres_server: postgres server address
    :param postgres_user: posygres username
    :param postgres_token: postgres password
    :param postgres_db: database where data to be persisted
    """
    log.info('Persisting dynaflo requests data into postgres db')

    if requests_data:
        try:
            with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                    postgres_db) as db_obj:
                conn, cursor = db_obj

                # getting required data from the requests data to persist in the database
                business_unit = requests_data.get('business_unit')
                organization = requests_data.get('organization')
                repository = requests_data.get('repository')
                branch = requests_data.get('branch')
                app_name = requests_data.get('app_name')
                env_type = requests_data.get('env_type')
                sub_folders = requests_data.get('sub_folders')
                first_commit = requests_data.get('first_used')
                latest_commit = requests_data.get('time')
                request_status = requests_data.get('status')
                request_message = requests_data.get('message')
                utc_time = requests_data.get('time')

                # update or insert into the table
                select_data = [constants.SELECT_ALL]
                where_data = {'business_unit': business_unit, 'organization': organization,
                              'repository': repository, 'branch': branch, 'app_name': app_name}
                select_query = postgres_utils.get_select_query(cursor, constants.DYNAFLO_REQUESTS_PSQL,
                                                               select_data, where_data)
                cursor.execute(select_query)
                data = cursor.fetchone()

                if not data:
                    insert_data = [business_unit, organization, repository, branch, app_name, sub_folders,
                                   env_type, first_commit, latest_commit, request_status, request_message, utc_time]
                    insert_query = postgres_utils.get_insert_query(cursor, constants.DYNAFLO_REQUESTS_PSQL, insert_data)
                    cursor.execute(insert_query)
                else:
                    update_data = {'sub_folders': sub_folders, 'env_type': env_type, 'latest_commit': latest_commit,
                                   'request_status': request_status, 'request_message': request_message, 'time': utc_time}
                    where_data = {'business_unit': business_unit, 'organization': organization,
                                  'repository': repository, 'branch': branch, 'app_name': app_name}
                    update_query = postgres_utils.get_update_query(cursor, constants.DYNAFLO_REQUESTS_PSQL, update_data, where_data)
                    cursor.execute(update_query)
                conn.commit()
                log.info('Successfully persisted dynaflo requests data into postgres db.')

        except Exception as ex:
            message = 'Error while persisting dynaflo requests data into postgres db: {}'.format(str(ex))
            log.exception(message)
            # raise MetricsError(message)
    else:
        message = 'Did not recevice any dynaflo requests data to persist into postgres db!'
        log.error(message)
        # raise MetricsError(message)


def get_unique_jobs_from_database(postgres_server: str, postgres_user: str, postgres_token: str, postgres_db: str,
                                  postgres_table: str) -> dict:
    """
    This function is to get the list of unique jobs from the database
    :param postgres_server: postgres server url
    :param postgres_user: postgres username
    :param postgres_token: postgres password
    :param postgres_db: postgres database name
    :param postgres_table: postgres table from which job names to be retrieved
    :return: A list of unique jobs in the databse
    """
    log.info('Getting a list of unique jobs from the database')
    unique_jobs = dict()

    try:
        with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                postgres_db) as db_obj:
            conn, cursor = db_obj

            # getting distinct region of jenkins server
            regions = list()
            select_data = ['DISTINCT(jenkins_region)']
            select_query = postgres_utils.get_select_query(cursor, postgres_table, select_data)
            cursor.execute(select_query)
            all_regions = cursor.fetchall()
            for reg in all_regions:
                temp = reg[0]
                if temp:
                    regions.append(temp)
            log.debug('Total unique jenkins regions: {}'.format(len(regions)))

            jobs_len = 0
            for region in regions:
                select_data = ['DISTINCT(job_name)']
                where_data = {'jenkins_region': region}
                select_query = postgres_utils.get_select_query(cursor, postgres_table, select_data,
                                                               where_data)
                cursor.execute(select_query)
                all_jobs = cursor.fetchall()

                temp_jobs = list()
                for job in all_jobs:
                    temp = job[0]
                    if temp:
                        temp_jobs.append(temp)
                jobs_len += len(temp_jobs)
                unique_jobs[region] = temp_jobs

        log.debug('Total unique jobs: {}'.format(jobs_len))
        log.debug(unique_jobs)
        return unique_jobs
    except Exception as ex:
        message = 'Error while getting the list of unique jobs from the database: {}'.format(str(ex))
        log.exception(message)
        # raise MetricsError(message)


def update_job_status_in_database(jobs_status: dict, postgres_server: str, postgres_user: str, postgres_token: str,
                                  postgres_db: str, postgres_table: str) -> None:
    """
    This function update the job status in the database
    :param jobs_status: A dictonary of jenkins jobs and its status
    :param postgres_server: postgres server url
    :param postgres_user: postgres username
    :param postgres_token: postgres password
    :param postgres_db: postgres database name
    :param postgres_table: postgres table from which job status to be updated
    :return: None
    """
    log.info('Updating jenkins job status in the database')

    if jobs_status:
        try:
            with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                    postgres_db) as db_obj:
                conn, cursor = db_obj

                for region, jobs in jobs_status.items():
                    for job, status in jobs.items():
                        try:
                            select_data = ['job_status']
                            where_data = {'job_name': job, 'jenkins_region': region}
                            select_query = postgres_utils.get_select_query(cursor, postgres_table, select_data,
                                                                           where_data)
                            cursor.execute(select_query)
                            current_status = cursor.fetchone()

                            if current_status and (status != current_status[0]):
                                utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)
                                update_data = {'job_status': status, 'last_updated_time': utc_time}
                                where_data = {'job_name': job, 'jenkins_region': region}
                                update_query = postgres_utils.get_update_query(cursor, postgres_table, update_data,
                                                                               where_data)
                                cursor.execute(update_query)
                        except Exception as ex:
                            message = 'Failed to execute update query: "{}".'.format(update_query) + \
                                    ' Error message: {}'.format(str(ex))
                            log.error(message)
                conn.commit()
        except Exception as ex:
            message = 'Error while updating jobs status in the database: {}'.format(str(ex))
            log.exception(message)
            # raise MetricsError(message)
    else:
        message = 'Did not receive any job status data to update the database!'
        log.error(message)
        # raise MetricsError(message)


def persist_into_psql_non_cicd_data(app_metrics: dict, postgres_server: str, postgres_user: str,
                                    postgres_token: str, postgres_db: str) -> None:
    """
    This function is to persist non-cicd application metrics into the postgres database
    :param app_metrics: metrics to be persisted
    :param postgres_server: postgres server
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    :return: None
    """
    log.info('Persisting non-cicd metrics into postgres db')
    log.debug('Gathered metrics: {}'.format(app_metrics))

    if app_metrics:
        try:
            with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                    postgres_db) as db_obj:
                conn, cursor = db_obj
                conn.autocommit = True

                job_url = app_metrics.get('job_url')
                job_name = app_metrics.get('job_name')
                build_status = app_metrics.get('build_status')
                time_taken = app_metrics.get('time_taken')
                trigger_cause = app_metrics.get('trigger_cause')
                jenkins_region = app_metrics.get('jenkins_region')
                automation_type = app_metrics.get('automation_type')
                job_status = 'active'
                utc_time = app_metrics.get('time')
                if not utc_time:
                    utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)

                select_data = [constants.SELECT_ALL]
                where_data = {'job_name': job_name, 'jenkins_region': jenkins_region}
                select_query = postgres_utils.get_select_query(cursor, constants.AR_APPS_NONCICD,
                                                               select_data, where_data)
                cursor.execute(select_query)
                data = cursor.fetchone()

                if not data:
                # insert new row
                    insert_data = {'job_url': job_url, 'job_name': job_name, 'build_status': build_status,
                                   'time_taken': time_taken, 'trigger_cause': trigger_cause, 'jenkins_region': jenkins_region,
                                   'automation_type': automation_type, 'last_triggered_time': utc_time,
                                   'job_status': job_status}
                    insert_query = postgres_utils.get_insert_query(cursor, constants.AR_APPS_NONCICD,
                                                                   insert_data)
                    cursor.execute(insert_query)
                else:
                    # update the existing row
                    where_data = {'job_name': job_name, 'jenkins_region': jenkins_region}
                    update_data = {'job_url': job_url, 'build_status': build_status, 'time_taken': time_taken,
                                   'trigger_cause': trigger_cause, 'last_triggered_time': utc_time,
                                   'job_status': job_status, 'automation_type': automation_type}
                    update_query = postgres_utils.get_update_query(cursor, constants.AR_APPS_NONCICD,
                                                                   update_data, where_data)
                    cursor.execute(update_query)
                log.info('Successfully persisted non-cicd metrics into the postgres database')
        except Exception as ex:
            message = 'Error while persisting non-cicd metrics into postgres db: {}'.format(str(ex))
            log.error(message)
            # # raise MetricsError(message)
    else:
        message = 'Did not receive any non-cicd metrics to persist into postgres db!'
        log.error(message)
        # raise MetricsError(message)


def persist_into_psql_deploy_all_data(app_metrics: dict, postgres_server: str, postgres_user: str,
                                      postgres_token: str, postgres_db: str) -> None:
    """
    This function is to persist deploy-all application metrics into the postgres database
    :param app_metrics: metrics to be persisted
    :param postgres_server: postgres server
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    :return: None
    """
    log.info('Persisting deploy-all metrics into postgres db')
    log.debug('Gathered metrics: {}'.format(app_metrics))

    if app_metrics:
        try:
            with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                    postgres_db) as db_obj:
                conn, cursor = db_obj
                conn.autocommit = True

                job_url = app_metrics.get('job_url')
                job_name = app_metrics.get('job_name')
                build_status = app_metrics.get('build_status')
                time_taken = app_metrics.get('time_taken')
                trigger_cause = app_metrics.get('trigger_cause')
                business_unit = app_metrics.get('business_unit')
                organization = app_metrics.get('organization')
                jenkins_region = app_metrics.get('jenkins_region')
                automation_type = app_metrics.get('automation_type')
                cicd_type = app_metrics.get('cicd_type')
                approval_jira = app_metrics.get('approval_jira')
                env_type = app_metrics.get('env_type')
                branch = app_metrics.get('branch')
                app_name = app_metrics.get('app_name')
                repository = app_metrics.get('repository')
                is_prod = app_metrics.get('is_prod', False)
                is_non_dev = app_metrics.get('is_non_dev', False)
                job_status = 'active'
                utc_time = app_metrics.get('time')
                if not utc_time:
                    utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)

                select_data = [constants.SELECT_ALL]
                where_data = {'jenkins_region': jenkins_region, 'business_unit': business_unit,
                              'organization': organization, 'cicd_type': cicd_type, 'job_name': job_name,
                              'repository': repository, 'branch': branch, 'app_name': app_name}
                select_query = postgres_utils.get_select_query(cursor, constants.AR_APPS, select_data,
                                                               where_data)
                cursor.execute(select_query)
                data = cursor.fetchone()

                if not data:
                # insert new row
                    insert_data = {'job_url': job_url, 'job_name': job_name, 'build_status': build_status,
                                   'time_taken': time_taken, 'trigger_cause': trigger_cause,
                                   'jenkins_region': jenkins_region, 'automation_type': automation_type,
                                   'last_triggered_time': utc_time, 'job_status': job_status,
                                   'business_unit': business_unit, 'organization': organization,
                                   'cicd_type': cicd_type, 'repository': repository,
                                   'branch': branch, 'app_name': app_name, 'approval_jira': approval_jira,
                                   'env_type': env_type, 'is_prod': is_prod, 'is_non_dev': is_non_dev}
                    insert_query = postgres_utils.get_insert_query(cursor, constants.AR_APPS, insert_data)
                    cursor.execute(insert_query)
                else:
                    # update the existing row
                    where_data = {'jenkins_region': jenkins_region, 'business_unit': business_unit,
                                  'organization': organization, 'cicd_type': cicd_type, 'job_name': job_name,
                                  'repository': repository, 'branch': branch, 'app_name': app_name}
                    update_data = {'job_url': job_url, 'build_status': build_status, 'time_taken': time_taken,
                                   'trigger_cause': trigger_cause, 'last_triggered_time': utc_time,
                                   'job_status': job_status, 'approval_jira': approval_jira,
                                   'env_type': env_type, 'automation_type': automation_type, 'is_prod': is_prod,
                                   'is_non_dev': is_non_dev}
                    update_query = postgres_utils.get_update_query(cursor, constants.AR_APPS, update_data,
                                                                   where_data)
                    cursor.execute(update_query)
                log.info('Successfully persisted deploy-all metrics into the postgres database')
        except Exception as ex:
            message = 'Error while persisting deploy-all metrics into postgres db: {}'.format(str(ex))
            log.error(message)
            # # raise MetricsError(message)
    else:
        message = 'Did not receive any deploy-all metrics to persist into postgres db!'
        log.error(message)
        # raise MetricsError(message)


def persist_into_psql_group_deploy_data(app_metrics: dict, postgres_server: str, postgres_user: str,
                                        postgres_token: str, postgres_db: str) -> None:
    """
    This function is to persist group deploy application metrics into the postgres database
    :param app_metrics: metrics to be persisted
    :param postgres_server: postgres server
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    :return: None
    """
    log.info('Persisting group deploy metrics into postgres db')
    log.debug('Gathered metrics: {}'.format(app_metrics))

    if app_metrics:
        try:
            with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                    postgres_db) as db_obj:
                conn, cursor = db_obj
                conn.autocommit = True

                job_url = app_metrics.get('job_url')
                job_name = app_metrics.get('job_name')
                build_status = app_metrics.get('build_status')
                time_taken = app_metrics.get('time_taken')
                trigger_cause = app_metrics.get('trigger_cause')
                business_unit = app_metrics.get('business_unit')
                organization = app_metrics.get('organization')
                repository = app_metrics.get('repository')
                branch = app_metrics.get('branch')
                jenkins_region = app_metrics.get('jenkins_region')
                automation_type = app_metrics.get('automation_type')
                cicd_type = app_metrics.get('cicd_type')
                job_status = 'active'
                utc_time = app_metrics.get('time')
                if not utc_time:
                    utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)

                select_data = [constants.SELECT_ALL]
                where_data = {'jenkins_region': jenkins_region, 'business_unit': business_unit,
                              'organization': organization, 'repository': repository, 'branch': branch,
                              'cicd_type': cicd_type, 'job_name': job_name, 'app_name': constants.NOT_AVAILABLE}
                select_query = postgres_utils.get_select_query(cursor, constants.AR_APPS, select_data,
                                                                where_data)
                cursor.execute(select_query)
                data = cursor.fetchone()

                if not data:
                    insert_data = {'job_url': job_url, 'job_name': job_name, 'build_status': build_status,
                                   'time_taken': time_taken, 'trigger_cause': trigger_cause,
                                   'jenkins_region': jenkins_region, 'automation_type': automation_type,
                                   'last_triggered_time': utc_time, 'job_status': job_status,
                                   'business_unit': business_unit, 'organization': organization,
                                   'branch': branch, 'cicd_type': cicd_type, 'repository': repository,
                                   'app_name': constants.NOT_AVAILABLE}
                    insert_query = postgres_utils.get_insert_query(cursor, constants.AR_APPS, insert_data)
                    cursor.execute(insert_query)
                else:
                    where_data = {'jenkins_region': jenkins_region, 'business_unit': business_unit,
                                  'organization': organization, 'repository': repository, 'branch': branch,
                                  'cicd_type': cicd_type, 'job_name': job_name, 'app_name': constants.NOT_AVAILABLE}
                    update_data = {'job_url': job_url, 'build_status': build_status, 'time_taken': time_taken,
                                   'trigger_cause': trigger_cause, 'last_triggered_time': utc_time,
                                   'job_status': job_status, 'automation_type': automation_type}
                    update_query = postgres_utils.get_update_query(cursor, constants.AR_APPS, update_data,
                                                                   where_data)
                    cursor.execute(update_query)
                log.info('Successfully persisted group deploy metrics into the postgres database')
        except Exception as ex:
            message = 'Error while persisting group deploy metrics into postgres db: {}'.format(str(ex))
            log.exception(message)
            # # raise MetricsError(message)
    else:
        message = 'Did not receive any group deploy metrics to persist into postgres db!'
        log.error(message)
        # raise MetricsError(message)


def persist_into_psql_pr_job_data(app_metrics: dict, postgres_server: str, postgres_user: str,
                                  postgres_token: str, postgres_db: str) -> None:
    """
    This function is to persist pull-request job metrics into the postgres database
    :param app_metrics: metrics to be persisted
    :param postgres_server: postgres server
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    :return: None
    """
    log.info('Persisting pr job metrics into postgres db')
    log.debug('Gathered metrics: {}'.format(app_metrics))

    if app_metrics:
        try:
            with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                    postgres_db) as db_obj:
                conn, cursor = db_obj
                conn.autocommit = True

                job_url = app_metrics.get('job_url')
                job_name = app_metrics.get('job_name')
                build_status = app_metrics.get('build_status')
                time_taken = app_metrics.get('time_taken')
                trigger_cause = app_metrics.get('trigger_cause')
                business_unit = app_metrics.get('business_unit')
                organization = app_metrics.get('organization')
                repository = app_metrics.get('repository')
                branch = app_metrics.get('branch')
                app_name = app_metrics.get('app_name')
                jenkins_region = app_metrics.get('jenkins_region')
                automation_type = app_metrics.get('automation_type')
                cicd_type = app_metrics.get('cicd_type')
                sonarqube_enabled = app_metrics.get('sonarqube_enabled')
                sonar_alert_status = app_metrics.get('alert_status')
                sonarqube_url = app_metrics.get('sonar_url')
                overall_code_coverage = app_metrics.get('coverage')
                new_code_coverage = app_metrics.get('new_coverage')
                reliability_rating = app_metrics.get('reliability_rating')
                maintainability_rating = app_metrics.get('sqale_rating')
                security_review_rating = app_metrics.get('security_review_rating')
                security_rating = app_metrics.get('security_rating')
                total_unit_tests = app_metrics.get('tests')
                test_failures = app_metrics.get('test_failures')
                test_errors = app_metrics.get('test_errors')
                test_success_density = app_metrics.get('test_success_density')
                skipped_tests = app_metrics.get('skipped_tests')
                test_execution_time = app_metrics.get('test_execution_time')
                squad = app_metrics.get('squad')
                domain = app_metrics.get('domain')
                fix_version = app_metrics.get('fix_version')
                job_status = 'active'
                utc_time = app_metrics.get('time')
                if not utc_time:
                    utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)

                select_data = [constants.SELECT_ALL]
                where_data = {'jenkins_region': jenkins_region, 'business_unit': business_unit,
                              'organization': organization, 'repository': repository, 'branch': branch,
                              'cicd_type': cicd_type, 'job_name': job_name, 'app_name': app_name}
                select_query = postgres_utils.get_select_query(cursor, constants.AR_APPS, select_data,
                                                               where_data)
                cursor.execute(select_query)
                data = cursor.fetchone()

                if not data:
                    insert_data = {'job_url': job_url, 'job_name': job_name, 'build_status': build_status,
                                   'time_taken': time_taken, 'trigger_cause': trigger_cause,
                                   'jenkins_region': jenkins_region, 'automation_type': automation_type,
                                   'last_triggered_time': utc_time, 'job_status': job_status,
                                   'business_unit': business_unit, 'organization': organization,
                                   'branch': branch, 'cicd_type': cicd_type, 'repository': repository,
                                   'app_name': app_name, 'sonarqube_enabled': sonarqube_enabled,
                                   'sonarqube_url': sonarqube_url, 'sonar_alert_status': sonar_alert_status,
                                   'overall_code_coverage': overall_code_coverage, 'reliability_rating': reliability_rating,
                                   'maintainability_rating': maintainability_rating, 'security_review_rating': security_review_rating,
                                   'security_rating': security_rating, 'squad': squad, 'domain': domain,
                                   'fix_version': fix_version, 'new_code_coverage' : new_code_coverage,
                                   'tests': total_unit_tests, 'test_failures': test_failures, 'test_errors': test_errors,
                                   'test_success_density': test_success_density,'skipped_tests': skipped_tests,
                                   'test_execution_time': test_execution_time}
                    insert_query = postgres_utils.get_insert_query(cursor, constants.AR_APPS, insert_data)
                    cursor.execute(insert_query)
                else:
                    where_data = {'jenkins_region': jenkins_region, 'business_unit': business_unit,
                                  'organization': organization, 'repository': repository, 'branch': branch,
                                  'cicd_type': cicd_type, 'job_name': job_name, 'app_name': app_name}
                    update_data = {'job_url': job_url, 'build_status': build_status, 'time_taken': time_taken,
                                   'trigger_cause': trigger_cause, 'last_triggered_time': utc_time,
                                   'job_status': job_status, 'automation_type': automation_type, 'sonarqube_enabled': sonarqube_enabled,
                                   'sonarqube_url': sonarqube_url, 'sonar_alert_status': sonar_alert_status,
                                   'overall_code_coverage': overall_code_coverage, 'reliability_rating': reliability_rating,
                                   'maintainability_rating': maintainability_rating, 'security_review_rating': security_review_rating,
                                   'security_rating': security_rating, 'squad': squad, 'domain': domain,
                                   'fix_version': fix_version, 'new_code_coverage' : new_code_coverage,'tests': total_unit_tests,
                                   'test_failures': test_failures, 'test_errors': test_errors, 'test_success_density': test_success_density,
                                   'skipped_tests': skipped_tests, 'test_execution_time': test_execution_time}
                    update_query = postgres_utils.get_update_query(cursor, constants.AR_APPS, update_data,
                                                                   where_data)
                    cursor.execute(update_query)
                log.info('Successfully persisted pr job metrics into the postgres database')
        except Exception as ex:
            message = 'Error while persisting pr job metrics into postgres db: {}'.format(str(ex))
            log.exception(message)
            # # raise MetricsError(message)
    else:
        message = 'Did not receive any pr job metrics to persist into postgres db!'
        log.error(message)
        # raise MetricsError(message)


def persist_into_psql_all_metrics_data(app_metrics: dict, postgres_server: str, postgres_user: str,
                                       postgres_token: str, postgres_db: str) -> None:
    """
    This function is to persist all metrics into the postgres database
    :param app_metrics: metrics to be persisted
    :param postgres_server: postgres server
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    :return: None
    """
    log.info('Persisting all metrics into postgres db')
    log.debug('Gathered metrics: {}'.format(app_metrics))

    if app_metrics:
        try:
            with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                    postgres_db) as db_obj:
                conn, cursor = db_obj
                conn.autocommit = True

                jenkins_region = app_metrics.get('jenkins_region')
                business_unit = app_metrics.get('business_unit')
                organization = app_metrics.get('organization')
                repository = app_metrics.get('repository')
                branch = app_metrics.get('branch')
                app_name = app_metrics.get('app_name')
                chear_id = app_metrics.get('chear_id', '')
                bsn = app_metrics.get('bsn', '')
                repo_url = app_metrics.get('repo_url')
                job_name = app_metrics.get('job_name')
                job_url = app_metrics.get('job_url')
                build_status = app_metrics.get('build_status')
                failed_stage = app_metrics.get('failed_stage')
                build_number = app_metrics.get('build_number')
                time_taken  = app_metrics.get('time_taken')
                trigger_cause = app_metrics.get('trigger_cause')
                automation_type = app_metrics.get('automation_type')
                cicd_type = app_metrics.get('cicd_type', constants.NOT_AVAILABLE)
                app_type = app_metrics.get('app_type')
                deploy_type = app_metrics.get('deploy_type')
                env_type = app_metrics.get('env_type')
                pipeline_type = app_metrics.get('pipeline_type')
                build_artifacts = app_metrics.get('build_artifacts')
                deploy_artifacts = app_metrics.get('deploy_artifacts')
                notification_users = app_metrics.get('notification_users')
                approval_jira = app_metrics.get('approval_jira')
                jira_from_commits = app_metrics.get('jira_from_commits')
                sonarqube_enabled = app_metrics.get('sonarqube_enabled')
                sonar_url = app_metrics.get('sonar_url')
                sonar_project_key = app_metrics.get('sonar_project_key')
                alert_status = app_metrics.get('alert_status')
                new_blocker_violations = app_metrics.get('new_blocker_violations')
                new_critical_violations = app_metrics.get('new_critical_violations')
                new_major_violations = app_metrics.get('new_major_violations')
                new_minor_violations = app_metrics.get('new_minor_violations')
                new_info_violations = app_metrics.get('new_info_violations')
                blocker_violations = app_metrics.get('blocker_violations')
                critical_violations = app_metrics.get('critical_violations')
                major_violations = app_metrics.get('major_violations')
                minor_violations = app_metrics.get('minor_violations')
                info_violations = app_metrics.get('info_violations')
                overall_code_coverage = app_metrics.get('coverage')
                new_code_coverage = app_metrics.get('new_coverage')
                reliability_rating = app_metrics.get('reliability_rating')
                maintainability_rating = app_metrics.get('sqale_rating')
                security_review_rating = app_metrics.get('security_review_rating')
                security_rating = app_metrics.get('security_rating')
                total_unit_tests = app_metrics.get('tests')
                test_failures = app_metrics.get('test_failures')
                test_errors = app_metrics.get('test_errors')
                test_success_density = app_metrics.get('test_success_density')
                skipped_tests = app_metrics.get('skipped_tests')
                test_execution_time = app_metrics.get('test_execution_time')
                unittest_enabled = app_metrics.get('unittest_enabled')
                twistlock_enabled = app_metrics.get('twistlock_enabled')
                docker_image = app_metrics.get('docker_image')
                total_vulnerabilities = app_metrics.get('total_vulnerabilities')
                checkmarx_enabled = app_metrics.get('checkmarx_enabled')
                checkmarx_status = app_metrics.get('checkmarx_status')
                checkmarx_report = app_metrics.get('checkmarx_report')
                checkmarx_failures = app_metrics.get('checkmarx_failures')
                checkmarx_async = app_metrics.get('checkmarx_async')
                metadata_enabled = app_metrics.get('metadata_enabled', False)
                is_first_build = app_metrics.get('is_first_build')
                is_prod = app_metrics.get('is_prod', False)
                is_non_dev = app_metrics.get('is_non_dev', False)
                is_dev = app_metrics.get('is_dev', False)
                is_multi = app_metrics.get('is_multi', False)
                repo_alias = app_metrics.get('repo_alias')
                squad = app_metrics.get('squad', constants.NOT_AVAILABLE)
                domain = app_metrics.get('domain', constants.NOT_AVAILABLE)
                fix_version = app_metrics.get('fix_version')
                utc_time = app_metrics.get('time')

                if not utc_time:
                    utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)
                    week = datetime.now(tz=timezone.utc).strftime("%U")
                    month = datetime.now(tz=timezone.utc).strftime("%b")
                    year = datetime.now(tz=timezone.utc).strftime("%Y")
                else:
                    if isinstance(utc_time, str):
                        try:
                            dto = datetime.strptime(utc_time,'%Y-%m-%d %H:%M:%S')
                        except Exception:
                            dto = datetime.strptime(utc_time,'%Y-%m-%d %H:%M:%S.%f')

                        week = dto.strftime('%U')
                        month = dto.strftime('%b')
                        year = dto.strftime('%Y')
                    else:
                        week = utc_time.strftime('%U')
                        month = utc_time.strftime('%b')
                        year = utc_time.strftime('%Y')

                select_data = [constants.SELECT_ALL]
                where_data = {'job_url': job_url, 'time': utc_time}
                select_query = postgres_utils.get_select_query(cursor, constants.AR_ALL_METRICS,
                                                               select_data, where_data)
                cursor.execute(select_query)
                data = cursor.fetchone()

                if not data:
                    insert_data = [jenkins_region, business_unit, organization, repository, branch, app_name,
                                chear_id, bsn, repo_url, job_name, job_url, build_status, failed_stage,
                                build_number, time_taken, trigger_cause, automation_type, cicd_type,
                                app_type, deploy_type, env_type, pipeline_type, build_artifacts, deploy_artifacts,
                                notification_users, approval_jira, jira_from_commits,
                                sonarqube_enabled, sonar_url, sonar_project_key, alert_status,
                                new_blocker_violations, new_critical_violations, new_major_violations,
                                new_minor_violations, new_info_violations, blocker_violations, critical_violations,
                                major_violations, minor_violations, info_violations, overall_code_coverage, reliability_rating,
                                maintainability_rating, security_review_rating, security_rating, unittest_enabled,
                                twistlock_enabled, docker_image, total_vulnerabilities, checkmarx_enabled,
                                checkmarx_status, checkmarx_report, checkmarx_failures, checkmarx_async,
                                metadata_enabled, is_first_build, is_prod, is_non_dev, is_dev, is_multi,
                                utc_time, repo_alias, squad, domain, fix_version, month, week, year, new_code_coverage,
                                total_unit_tests, test_failures, test_errors, test_success_density, skipped_tests,
                                test_execution_time]
                    insert_query = postgres_utils.get_insert_query(cursor, constants.AR_ALL_METRICS, insert_data)
                    cursor.execute(insert_query)
                else:
                    update_data = {
                        'jenkins_region': jenkins_region, 'business_unit': business_unit,
                        'organization': organization, 'repository': repository, 'branch': branch,
                        'app_name': app_name, 'chear_id': chear_id, 'bsn': bsn, 'repo_url': repo_url,
                        'job_name': job_name, 'build_status': build_status, 'failed_stage': failed_stage,
                        'build_number': build_number, 'time_taken': time_taken, 'trigger_cause': trigger_cause,
                        'automation_type': automation_type, 'cicd_type': cicd_type, 'app_type': app_type,
                        'deploy_type': deploy_type, 'env_type': env_type, 'pipeline_type': pipeline_type,
                        'build_artifacts': build_artifacts, 'deploy_artifacts': deploy_artifacts,
                        'notification_users': notification_users, 'approval_jira': approval_jira,
                        'jira_from_commits': jira_from_commits, 'sonarqube_enabled': sonarqube_enabled,
                        'sonar_url': sonar_url, 'sonar_project_key': sonar_project_key, 'alert_status': alert_status,
                        'new_blocker_violations': new_blocker_violations, 'new_critical_violations': new_critical_violations,
                        'new_major_violations': new_major_violations, 'new_minor_violations': new_minor_violations,
                        'new_info_violations': new_info_violations, 'blocker_violations': blocker_violations,
                        'critical_violations': critical_violations, 'major_violations': major_violations,
                        'minor_violations': minor_violations, 'info_violations': info_violations,
                        'overall_code_coverage': overall_code_coverage, 'reliability_rating': reliability_rating,
                        'maintainability_rating': maintainability_rating, 'security_review_rating': security_review_rating,
                        'security_rating': security_rating, 'unittest_enabled': unittest_enabled,
                        'twistlock_enabled': twistlock_enabled, 'docker_image': docker_image,
                        'total_vulnerabilities': total_vulnerabilities, 'checkmarx_enabled': checkmarx_enabled,
                        'checkmarx_status': checkmarx_status, 'checkmarx_report': checkmarx_report,
                        'checkmarx_failures': checkmarx_failures, 'checkmarx_async': checkmarx_async,
                        'metadata_enabled': metadata_enabled, 'is_first_build': is_first_build,
                        'is_prod': is_prod, 'is_non_dev': is_non_dev, 'is_dev': is_dev, 'is_multi': is_multi,
                        'repo_alias': repo_alias, 'squad': squad, 'domain': domain, 'fix_version': fix_version,
                        'month': month, 'week': week, 'year': year, 'new_code_coverage': new_code_coverage,
                        'tests': total_unit_tests, 'test_failures': test_failures, 'test_errors': test_errors,
                        'test_success_density': test_success_density,'skipped_tests': skipped_tests,
                        'test_execution_time': test_execution_time
                    }
                    where_data = {'job_url': job_url, 'time': utc_time}
                    update_query = postgres_utils.get_update_query(cursor, constants.AR_ALL_METRICS,
                                                                   update_data, where_data)
                    cursor.execute(update_query)
                log.info('Successfully persisted all metrics into the postgres database')
        except Exception as ex:
            message = 'Error while persisting all metrics into postgres db: {}'.format(str(ex))
            log.exception(message)
            # # raise MetricsError(message)
    else:
        message = 'Did not receive any metrics to persist into postgres db!'
        log.error(message)
        # raise MetricsError(message)


def persist_into_psql_cicd_data(app_metrics: dict, postgres_server: str, postgres_user: str,
                                postgres_token: str, postgres_db: str) -> None:
    """
    This function is to persist the cicd metrics into the postgres database
    :param app_metrics: metrics to be persisted
    :param postgres_server: postgres server
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    :return: None
    """
    log.info('Persisting cicd metrics into postgres db')

    if app_metrics:
        try:
            with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                    postgres_db) as db_obj:
                conn, cursor = db_obj
                conn.autocommit = True

                automation_type = app_metrics.get('automation_type')
                job_name = app_metrics.get('job_name')
                jenkins_region = app_metrics.get('jenkins_region')
                job_url = app_metrics.get('job_url')
                build_status = app_metrics.get('build_status')
                time_taken = app_metrics.get('time_taken')
                trigger_cause = app_metrics.get('trigger_cause')
                cicd_type = app_metrics.get('cicd_type', constants.NOT_AVAILABLE)
                business_unit = app_metrics.get('business_unit')
                organization = app_metrics.get('organization')
                branch = app_metrics.get('branch')
                repository = app_metrics.get('repository')
                repo_alias = app_metrics.get('repo_alias')
                job_status = 'active'
                utc_time = app_metrics.get('time')
                app_name = app_metrics.get('app_name')
                chear_id = app_metrics.get('chear_id', '')
                bsn = app_metrics.get('bsn', '')
                env_type = app_metrics.get('env_type')
                app_type = app_metrics.get('app_type')
                deploy_type = app_metrics.get('deploy_type')
                pipeline_type = app_metrics.get('pipeline_type')
                build_artifacts = app_metrics.get('build_artifacts')
                deploy_artifacts = app_metrics.get('deploy_artifacts')
                sonarqube_enabled = app_metrics.get('sonarqube_enabled')
                sonarqube_url = app_metrics.get('sonar_url')
                sonar_alert_status = app_metrics.get('alert_status')
                twistlock_enabled = app_metrics.get('twistlock_enabled')
                docker_image = app_metrics.get('docker_image')
                total_vulnerabilities = app_metrics.get('total_vulnerabilities')
                unittest_enabled = app_metrics.get('unittest_enabled')
                checkmarx_enabled = app_metrics.get('checkmarx_enabled')
                checkmarx_status = app_metrics.get('checkmarx_status')
                checkmarx_report = app_metrics.get('checkmarx_report')
                checkmarx_failures = app_metrics.get('checkmarx_failures')
                is_dev = app_metrics.get('is_dev', False)
                is_non_dev = app_metrics.get('is_non_dev', False)
                is_prod = app_metrics.get('is_prod', False)
                is_multi = app_metrics.get('is_multi', False)
                notification_users = app_metrics.get('notification_users')
                metadata_enabled = app_metrics.get('metadata_enabled', False)
                approval_jira = app_metrics.get('approval_jira')
                jira_from_commits = app_metrics.get('jira_from_commits')
                overall_code_coverage = app_metrics.get('coverage')
                new_code_coverage = app_metrics.get('new_coverage')
                reliability_rating = app_metrics.get('reliability_rating')
                maintainability_rating = app_metrics.get('sqale_rating')
                security_review_rating = app_metrics.get('security_review_rating')
                security_rating = app_metrics.get('security_rating')
                total_unit_tests = app_metrics.get('tests')
                test_failures = app_metrics.get('test_failures')
                test_errors = app_metrics.get('test_errors')
                test_success_density = app_metrics.get('test_success_density')
                skipped_tests = app_metrics.get('skipped_tests')
                test_execution_time = app_metrics.get('test_execution_time')
                squad = app_metrics.get('squad', constants.NOT_AVAILABLE)
                domain = app_metrics.get('domain', constants.NOT_AVAILABLE)
                fix_version = app_metrics.get('fix_version')
                if not utc_time:
                    utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)

                # inserting/updating into the main table
                if env_type not in ['multi']:
                    select_data = [constants.SELECT_ALL]
                    where_data = {'business_unit': business_unit, 'jenkins_region': jenkins_region,
                                  'organization': organization, 'repository': repository, 'app_name': app_name,
                                  'branch': branch, 'is_multi': False, 'job_name': job_name}
                    select_query = postgres_utils.get_select_query(cursor, constants.AR_APPS, select_data,
                                                                   where_data)
                    cursor.execute(select_query)
                    data = cursor.fetchone()

                    if not data:
                        # insert new row
                        insert_data = [business_unit, jenkins_region, organization, repository,
                                       branch, app_name, chear_id, bsn, job_name, job_status,
                                       job_url, build_status, time_taken, trigger_cause, env_type,
                                       app_type, deploy_type, pipeline_type, build_artifacts,
                                       deploy_artifacts, sonarqube_enabled, sonarqube_url,
                                       sonar_alert_status, twistlock_enabled, docker_image,
                                       total_vulnerabilities, unittest_enabled, checkmarx_enabled,
                                       checkmarx_status, checkmarx_report, checkmarx_failures,
                                       is_dev, is_non_dev, is_prod, is_multi, None, metadata_enabled,
                                       notification_users, utc_time, approval_jira, automation_type,
                                       cicd_type, jira_from_commits, overall_code_coverage,
                                       reliability_rating, maintainability_rating,
                                       security_review_rating, security_rating, repo_alias, squad,
                                       domain, fix_version, new_code_coverage, total_unit_tests,
                                       test_failures, test_errors, test_success_density,
                                       skipped_tests, test_execution_time]
                        insert_query = postgres_utils.get_insert_query(cursor, constants.AR_APPS, insert_data)
                        cursor.execute(insert_query)
                    else:
                        # update few data
                        update_data = {'chear_id': chear_id, 'bsn': bsn, 'job_url': job_url, 'build_status': build_status,
                                       'time_taken': time_taken, 'trigger_cause': trigger_cause, 'env_type': env_type,
                                       'app_type': app_type, 'deploy_type': deploy_type, 'pipeline_type': pipeline_type,
                                       'build_artifacts': build_artifacts, 'deploy_artifacts': deploy_artifacts,
                                       'sonarqube_enabled': sonarqube_enabled, 'sonarqube_url': sonarqube_url,
                                       'sonar_alert_status': sonar_alert_status, 'twistlock_enabled': twistlock_enabled,
                                       'docker_image': docker_image, 'total_vulnerabilities': total_vulnerabilities,
                                       'unittest_enabled': unittest_enabled, 'checkmarx_enabled': checkmarx_enabled,
                                       'checkmarx_status': checkmarx_status, 'checkmarx_report': checkmarx_report,
                                       'checkmarx_failures': checkmarx_failures, 'last_triggered_time': utc_time,
                                       'job_status': job_status, 'is_dev': is_dev, 'is_non_dev': is_non_dev,
                                       'is_prod': is_prod, 'is_multi': is_multi, 'metadata_enabled': metadata_enabled,
                                       'notification_users': notification_users, 'approval_jira': approval_jira,
                                       'cicd_type': cicd_type, 'jira_from_commits': jira_from_commits,
                                       'overall_code_coverage': overall_code_coverage, 'reliability_rating': reliability_rating,
                                       'maintainability_rating': maintainability_rating, 'security_review_rating': security_review_rating,
                                       'security_rating': security_rating, 'repo_alias':repo_alias,
                                       'squad': squad, 'domain': domain, 'fix_version': fix_version, 'new_code_coverage' : new_code_coverage,
                                       'tests': total_unit_tests, 'test_failures': test_failures, 'test_errors': test_errors, 'test_success_density': test_success_density,
                                        'skipped_tests': skipped_tests,'test_execution_time': test_execution_time}
                        where_data = {'business_unit': business_unit, 'jenkins_region': jenkins_region,
                                      'organization': organization, 'repository': repository, 'app_name': app_name,
                                      'branch': branch, 'is_multi': False, 'job_name': job_name}
                        update_query = postgres_utils.get_update_query(cursor, constants.AR_APPS,
                                                                        update_data, where_data)
                        cursor.execute(update_query)
                else: # for multibranch data, we update branch also
                    select_data = [constants.SELECT_ALL]
                    where_data = {'business_unit': business_unit, 'jenkins_region': jenkins_region,
                                    'organization': organization, 'repository': repository, 'app_name': app_name,
                                    'is_multi': True, 'job_name': job_name}
                    select_query = postgres_utils.get_select_query(cursor, constants.AR_APPS,
                                                                    select_data, where_data)
                    cursor.execute(select_query)
                    data = cursor.fetchone()

                    if not data:
                        # insert new row
                        insert_data = [business_unit, jenkins_region, organization, repository, branch,
                                       app_name, chear_id, bsn, job_name, job_status, job_url, build_status,
                                       time_taken, trigger_cause, env_type, app_type, deploy_type, pipeline_type,
                                       build_artifacts, deploy_artifacts, sonarqube_enabled,
                                       sonarqube_url, sonar_alert_status, twistlock_enabled,
                                       docker_image, total_vulnerabilities, unittest_enabled,
                                       checkmarx_enabled, checkmarx_status, checkmarx_report,
                                       checkmarx_failures, is_dev, is_non_dev,
                                       is_prod, is_multi, None, metadata_enabled, notification_users,
                                       utc_time, approval_jira, automation_type, cicd_type,
                                       jira_from_commits, overall_code_coverage, reliability_rating,
                                       maintainability_rating, security_review_rating,
                                       security_rating, repo_alias, squad, domain, fix_version,
                                       new_code_coverage, total_unit_tests, test_failures, test_errors,
                                       test_success_density, skipped_tests, test_execution_time]
                        insert_query = postgres_utils.get_insert_query(cursor, constants.AR_APPS, insert_data)
                        cursor.execute(insert_query)
                    else:
                        # update few data
                        update_data = {'branch': branch, 'chear_id': chear_id, 'bsn': bsn, 'job_url': job_url,
                                       'build_status': build_status, 'time_taken': time_taken, 'trigger_cause': trigger_cause,
                                       'env_type': env_type, 'app_type': app_type, 'deploy_type': deploy_type,
                                       'pipeline_type': pipeline_type, 'build_artifacts': build_artifacts,
                                       'deploy_artifacts': deploy_artifacts, 'sonarqube_enabled': sonarqube_enabled,
                                       'sonarqube_url': sonarqube_url, 'sonar_alert_status': sonar_alert_status,
                                       'twistlock_enabled': twistlock_enabled, 'docker_image': docker_image,
                                       'total_vulnerabilities': total_vulnerabilities, 'unittest_enabled': unittest_enabled,
                                       'checkmarx_enabled': checkmarx_enabled, 'checkmarx_status': checkmarx_status,
                                       'checkmarx_report': checkmarx_report, 'checkmarx_failures': checkmarx_failures,
                                       'last_triggered_time': utc_time, 'job_status': job_status, 'is_dev': is_dev,
                                       'is_non_dev': is_non_dev, 'is_prod': is_prod, 'is_multi': is_multi,
                                       'metadata_enabled': metadata_enabled, 'notification_users': notification_users,
                                       'cicd_type': cicd_type, 'jira_from_commits':jira_from_commits,
                                       'overall_code_coverage': overall_code_coverage, 'reliability_rating': reliability_rating,
                                       'maintainability_rating': maintainability_rating, 'security_review_rating': security_review_rating,
                                       'security_rating': security_rating, 'repo_alias': repo_alias,
                                       'squad': squad, 'domain': domain, 'fix_version': fix_version, 'new_code_coverage' : new_code_coverage,
                                       'tests': total_unit_tests, 'test_failures': test_failures, 'test_errors': test_errors,
                                       'test_success_density': test_success_density,'skipped_tests': skipped_tests,
                                       'test_execution_time': test_execution_time}
                        where_data = {'business_unit': business_unit, 'jenkins_region': jenkins_region,
                                      'organization': organization, 'repository': repository, 'app_name': app_name,
                                      'is_multi': True, 'job_name': job_name}
                        update_query = postgres_utils.get_update_query(cursor, constants.AR_APPS, update_data,
                                                                       where_data)
                        cursor.execute(update_query)
                log.info('Successfully persisted cicd data into the postgres database')
        except Exception as ex:
            message = 'Error while persisting cicd metrics into postgres db: {}'.format(str(ex))
            log.exception(message)
            # # raise MetricsError(message)
    else:
        message = 'Did not receive any cicd metrics to persist into postgres db!'
        log.error(message)
        # raise MetricsError(message)


def persist_into_psql_trends_data(app_metrics: dict, postgres_server: str, postgres_user: str,
                                  postgres_token: str, postgres_db: str) -> None:
    """
    This function is to persist metrics trends into the postgres database
    :param app_metrics: metrics to be persisted
    :param postgres_server: postgres server
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    :return: None
    """
    log.info('Persisting metrics trends into postgres db')

    if app_metrics:
        try:
            with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                    postgres_db) as db_obj:
                conn, cursor = db_obj
                conn.autocommit = True

                automation_type = app_metrics.get('automation_type')
                job_name = app_metrics.get('job_name')
                jenkins_region = app_metrics.get('jenkins_region')
                build_status = app_metrics.get('build_status')
                business_unit = app_metrics.get('business_unit')
                organization = app_metrics.get('organization')
                branch = app_metrics.get('branch')
                repository = app_metrics.get('repository')
                job_status = 'active'
                utc_time = app_metrics.get('time')
                app_name = app_metrics.get('app_name')
                env_type = app_metrics.get('env_type')
                pipeline_type = app_metrics.get('pipeline_type')
                failed_stage = app_metrics.get('failed_stage')
                dev_build_num = app_metrics.get('dev_build_num')
                if not utc_time:
                    utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)

                # insert into jenkins failures table and for failure analysis
                if build_status == 'Failure' and failed_stage:
                    # do a little bit of preprocessing
                    if re.match('^SCM: Branch not suitable for integration as it does not merge cleanly.*',
                                failed_stage):
                        failed_stage = 'SCM: Branch not suitable for integration as it does not merge cleanly'
                    elif re.match('^Preload: .*app\.yml does not exist\.$', failed_stage):
                        failed_stage = 'Preload: pipeline_override.yml does not exist.'
                    elif re.match("^Build, create artifact and upload to nexus for higher envs: .*\.zip file doesn't exists$",
                                    failed_stage):
                        failed_stage = "Build, create artifact and upload to nexus for higher envs: java.io.IOException: zip file doesn't exists"
                    elif re.match("^Uploading Artifacts to nexus: .*\.zip file doesn't exists$", failed_stage):
                        failed_stage = "Uploading Artifacts to nexus: zip file doesn't exists"
                    elif re.match("^Uploading Artifacts to nexus: Uploading file .*\.zip failed\.$",
                                    failed_stage):
                        failed_stage = "Uploading Artifacts to nexus: Uploading zip file failed."
                    elif re.match("^Sonar Scan: java\.util\.concurrent\.ExecutionException: groovy\.lang\.MissingMethodException: No signature of method.*$",
                                  failed_stage):
                        failed_stage = "Sonar Scan: java.util.concurrent.ExecutionException: groovy.lang.MissingMethodException: No signature of method"
                    elif re.match("^Artifact Upload: .*\.zip file doesn't exists$", failed_stage):
                        failed_stage = "Artifact Upload: zip file doesn't exists"
                    elif re.match("^Upload: Uploading file .*\.zip failed.$", failed_stage):
                        failed_stage = "Upload: Uploading zip file failed."

                    stage = failed_stage.split(': ')[0]
                    reason = ': '.join(failed_stage.split(': ')[1:])
                    insert_data = [business_unit, jenkins_region, organization, repository, app_name,
                                   branch, env_type, job_name, stage, reason, utc_time]
                    insert_query = postgres_utils.get_insert_query(cursor, constants.AR_JENKINS_FAILURES,
                                                                   insert_data)
                    cursor.execute(insert_query)
                    log.info('Successfully persisted into jenkins failures table')

                # for prod environments, get the total time taken to promote from dev environments
                # and total builds taken to promote from dev to prod
                if env_type == 'prod' and pipeline_type not in [constants.PIPELINE_SCALABLE, constants.UNCATEGORIZED]:
                    select_data = [constants.SELECT_ALL]
                    where_data = {'business_unit': business_unit, 'jenkins_region': jenkins_region,
                                  'organization': organization, 'repository': repository, 'app_name': app_name,
                                  'branch': branch}
                    select_query = postgres_utils.get_select_query(cursor, constants.AR_DEV_TO_PROD,
                                                                   select_data, where_data)
                    cursor.execute(select_query)
                    data = cursor.fetchone()

                    if not data:
                        time_taken_to_promote = '0 days 0.000000 seconds'
                        builds_taken_to_promote = 0
                        insert_data = [business_unit, jenkins_region, organization, repository, app_name,
                                       branch, pipeline_type, utc_time, dev_build_num, utc_time, dev_build_num,
                                       time_taken_to_promote, builds_taken_to_promote, utc_time]
                        insert_query = postgres_utils.get_insert_query(cursor, constants.AR_DEV_TO_PROD,
                                                                       insert_data)
                        cursor.execute(insert_query)
                    else:
                        previous_deployment_date = data[9]
                        previous_build_number = data[10]
                        time_taken_to_promote = utc_time - previous_deployment_date
                        build_taken_to_promote = dev_build_num - previous_build_number

                        update_data = {'previous_deployment': previous_deployment_date, 'previous_build_num': previous_build_number,
                                       'current_deployment': utc_time, 'current_build_num': dev_build_num,
                                       'time_taken_to_promote': time_taken_to_promote, 'builds_taken_to_promote': build_taken_to_promote,
                                       'time': utc_time}
                        where_data = {'business_unit': business_unit, 'jenkins_region': jenkins_region,
                                      'organization': organization, 'repository': repository, 'app_name': app_name,
                                      'branch': branch}
                        update_query = postgres_utils.get_update_query(cursor, constants.AR_DEV_TO_PROD,
                                                                       update_data, where_data)
                        cursor.execute(update_query)
                    log.info('Successfully persisted into dev to prod promotion table')

                utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)
                # this is for onboarding applications trend
                select_data = [constants.COUNT_DISTINCT_REPO]
                where_data = {'job_status': job_status, 'automation_type': automation_type}
                select_query = postgres_utils.get_select_query(cursor, constants.AR_APPS, select_data,
                                                               where_data)
                cursor.execute(select_query)
                app_count = cursor.fetchone()[0]

                insert_data = [app_count, utc_time]
                insert_query = postgres_utils.get_insert_query(cursor, constants.AR_APP_COUNT,
                                                               insert_data)
                cursor.execute(insert_query)

                # this is for onboarding application trends for pipelines
                select_data = [constants.COUNT_DISTINCT_BR]
                where_data = {'job_status': job_status, 'automation_type': automation_type}
                select_query = postgres_utils.get_select_query(cursor, constants.AR_APPS, select_data,
                                                               where_data)
                cursor.execute(select_query)
                app_count = cursor.fetchone()[0]

                insert_data = [app_count, utc_time]
                insert_query = postgres_utils.get_insert_query(cursor, constants.AR_APP_COUNT_PIPELINE,
                                                               insert_data)
                cursor.execute(insert_query)
                log.info('Successfully persisted into applications trend tables')

                # this is for sonarqube trends
                # count of sonar scan enabled applications
                prepared_query = """SELECT {} FROM {} WHERE job_status=%s AND sonarqube_enabled=%s \
                    AND automation_type=%s AND (is_dev=%s OR is_multi=%s) \
                    AND (cicd_type IS NULL
                            OR cicd_type NOT IN (%s, %s, %s));""".format(constants.COUNT_DISTINCT_REPO,
                                                                         constants.AR_APPS)
                select_query = cursor.mogrify(prepared_query, [job_status, 'true', automation_type, True,
                                                               True, constants.GROUP_DEPLOY, constants.DEPLOY_ALL,
                                                               constants.PR_JOB])
                log.debug('Select query: {}'.format(select_query))
                cursor.execute(select_query)
                app_count = cursor.fetchone()[0]

                insert_data = ['Enabled', app_count, utc_time]
                insert_query = postgres_utils.get_insert_query(cursor, constants.AR_SONARQUBE_TRENDS,
                                                               insert_data)
                cursor.execute(insert_query)

                # count of sonar scan disabled applications
                prepared_query = """SELECT {} FROM {} WHERE job_status=%s AND sonarqube_enabled=%s \
                    AND automation_type=%s AND (is_dev=%s OR is_multi=%s) \
                    AND (cicd_type IS NULL
                            OR cicd_type NOT IN (%s, %s, %s));""".format(constants.COUNT_DISTINCT_REPO,
                                                                     constants.AR_APPS)
                select_query = cursor.mogrify(prepared_query, [job_status, 'false', automation_type, True,
                                                               True, constants.GROUP_DEPLOY, constants.DEPLOY_ALL,
                                                               constants.PR_JOB])
                log.debug('Select query: {}'.format(select_query))
                cursor.execute(select_query)
                app_count = cursor.fetchone()[0]

                insert_data = ['Disabled', app_count, utc_time]
                insert_query = postgres_utils.get_insert_query(cursor, constants.AR_SONARQUBE_TRENDS,
                                                               insert_data)
                cursor.execute(insert_query)
                log.info('Successfully persisted into sonarqube trends table')

                # this is for twistlock trends
                prepared_query = """SELECT {} FROM {} WHERE job_status=%s AND twistlock_enabled=%s \
                    AND automation_type=%s \
                    AND (cicd_type IS NULL
                            OR cicd_type NOT IN (%s, %s, %s));""".format(constants.COUNT_DISTINCT_REPO,
                                                                     constants.AR_APPS)
                select_query = cursor.mogrify(prepared_query, [job_status, True, automation_type,
                                                               constants.GROUP_DEPLOY, constants.DEPLOY_ALL,
                                                               constants.PR_JOB])
                log.debug('Select query: {}'.format(select_query))
                cursor.execute(select_query)
                app_count = cursor.fetchone()[0]

                insert_data = [app_count, utc_time]
                insert_query = postgres_utils.get_insert_query(cursor, constants.AR_TWISTLOCK_TRENDS,
                                                               insert_data)
                cursor.execute(insert_query)
                log.info('Successfully persisted into twistlock trends table')

                # this is for checkmarx trends
                # count of checkmarx enabled applications
                prepared_query = """SELECT {} FROM {} WHERE job_status=%s AND checkmarx_enabled=%s \
                    AND automation_type=%s AND (is_dev=%s OR is_multi=%s) \
                    AND (cicd_type IS NULL
                            OR cicd_type NOT IN (%s, %s, %s));""".format(constants.COUNT_DISTINCT_REPO,
                                                                     constants.AR_APPS)
                select_query = cursor.mogrify(prepared_query, [job_status, True, automation_type, True,
                                                               True, constants.GROUP_DEPLOY, constants.DEPLOY_ALL,
                                                               constants.PR_JOB])
                log.debug('Select query: {}'.format(select_query))
                cursor.execute(select_query)
                app_count = cursor.fetchone()[0]

                insert_data = ['Enabled', app_count, utc_time]
                insert_query = postgres_utils.get_insert_query(cursor, constants.AR_CHECKMARX_TRENDS,
                                                               insert_data)
                cursor.execute(insert_query)

                # this is for Unique APM counts trends for pipelines
                select_data = [constants.COUNT_DISTINCT_APM]
                select_query = postgres_utils.get_select_query(cursor, constants.AR_APPS, select_data)
                cursor.execute(select_query)
                unique_apm_count = cursor.fetchone()[0]

                utc_today = datetime.utcnow().date()

                select_query = f"SELECT unique_apm_count, time FROM {constants.AR_APM_TRENDS} WHERE Date(time)='{utc_today}' ORDER BY time DESC LIMIT 1;"
                cursor.execute(select_query)
                current_entry = cursor.fetchone()

                if current_entry is not None:
                    # An entry already exists for today's date, and existing unique apm count and
                    # current count doesn't match then Insert a new entry
                    unique_apm_count_trend = current_entry[0]

                    if unique_apm_count_trend != unique_apm_count:
                        insert_data = [unique_apm_count, utc_time]
                        insert_query = postgres_utils.get_insert_query(cursor, constants.AR_APM_TRENDS, insert_data)
                        cursor.execute(insert_query)
                    else:
                        log.info('Unique apm count has not changed')

                else:
                    # No entry exists for today's date, insert a new row with count and time
                    insert_data = [unique_apm_count, utc_time]
                    insert_query = postgres_utils.get_insert_query(cursor, constants.AR_APM_TRENDS, insert_data)
                    cursor.execute(insert_query)
                log.info('Successfully inserted into unique apm trends table')

                # count of checkmarx disabled applications
                prepared_query = """SELECT {} FROM {} WHERE job_status=%s AND checkmarx_enabled=%s \
                    AND automation_type=%s AND (is_dev=%s OR is_multi=%s) \
                    AND (cicd_type IS NULL
                            OR cicd_type NOT IN (%s, %s, %s));""".format(constants.COUNT_DISTINCT_REPO,
                                                                     constants.AR_APPS)
                select_query = cursor.mogrify(prepared_query, [job_status, False, automation_type, True,
                                                               True, constants.GROUP_DEPLOY, constants.DEPLOY_ALL,
                                                               constants.PR_JOB])
                log.debug('Select query: {}'.format(select_query))
                cursor.execute(select_query)
                app_count = cursor.fetchone()[0]

                insert_data = ['Disabled', app_count, utc_time]
                insert_query = postgres_utils.get_insert_query(cursor, constants.AR_CHECKMARX_TRENDS,
                                                               insert_data)
                cursor.execute(insert_query)
                log.info('Successfully persisted into checkmarx trends table')

                # this is for applications pipelines trend
                for key in [constants.PIPELINE_MANAGED, constants.PIPELINE_SCM, constants.PIPELINE_CUSTOM,
                            constants.PIPELINE_SCALABLE, constants.ONE_TEMPLATE, constants.UNCATEGORIZED]:
                    select_data = [constants.COUNT_DISTINCT_BR]
                    where_data = {'pipeline_type': key, 'job_status': job_status, 'automation_type': automation_type}
                    select_query = postgres_utils.get_select_query(cursor, constants.AR_APPS, select_data,
                                                                   where_data)
                    cursor.execute(select_query)
                    app_count = cursor.fetchone()[0]

                    insert_data = [key, app_count, utc_time]
                    insert_query = postgres_utils.get_insert_query(cursor, constants.AR_PIPELINE_TRENDS,
                                                                   insert_data)
                    cursor.execute(insert_query)
                log.info('Successfully persisted into application pipeline trends table')
                log.info('Successfully persisted metrics trends into postgres db')
        except Exception as ex:
            message = 'Error while persisting metrics trends into postgres db: {}'.format(str(ex))
            log.exception(message)
            # # raise MetricsError(message)
    else:
        message = 'Did not receive any metrics trends to persist into postgres db!'
        log.error(message)
        # raise MetricsError(message)


def persist_node_usage_into_postgres(jenkins_metrics: dict, node_usage: list, postgres_server: str,
                                     postgres_user: str, postgres_token: str, postgres_db: str) -> None:
    """
    This function is to persist node utilization data into the postgres database
    :param jenkins_metrics: gathered jenkins metrics
    :param node_usage: nodes and its utilization
    :param postgres_server: postgres server name
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    """
    log.info('Persisting node utilization data into the postgres database')

    try:
        with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                postgres_db) as db_obj:
            conn, cursor = db_obj
            conn.autocommit = True

            bu = jenkins_metrics.get('business_unit')
            jenkins_region = jenkins_metrics.get('jenkins_region')
            organization = jenkins_metrics.get('organization')
            repository = jenkins_metrics.get('repository')
            branch = jenkins_metrics.get('branch')
            app_name = jenkins_metrics.get('app_name')
            job_name = jenkins_metrics.get('job_name')
            utc_time = jenkins_metrics.get('time')

            if not utc_time:
                utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)

            for row in node_usage:
                insert_data = [bu, jenkins_region, organization, repository, branch, app_name, job_name, row.get('node'),
                               row.get('time_taken'), utc_time]
                insert_query = postgres_utils.get_insert_query(cursor, constants.AR_NODE_USAGE, insert_data)
                cursor.execute(insert_query)
            log.info('Successfully persisted node utilization data into the database.')
    except Exception as ex:
        message = 'Error while persisting nodes usage into the database: {}'.format(str(ex))
        log.exception(message)
        # raise MetricsError(message)


def persist_gitleaks_metrics_postgres(gitleak_metrics: list, postgres_server: str,
                                      postgres_user: str, postgres_token: str,
                                     postgres_db: str) -> Tuple[str, int]:
    """
    This function is to persist gitleak metrics data into the postgres database
    :param gitleak_metrics: gathered gitleak metrics
    :param postgres_server: postgres server name
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    """
    log.info('Persisting gitleak metrics data into the postgres database')

    try:
        with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                postgres_db) as db_obj:
            conn, cursor = db_obj
            conn.autocommit = True

            all_rows_inserted = True
            for row in gitleak_metrics:
                try:
                    date_value = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)
                    build_url = row.get('build_URL')
                    rule_id = row.get('RuleID')
                    commit = row.get('Commit')
                    file_name = row.get('File')
                    secret = row.get('Secret')
                    match = row.get('Match')
                    start_line = row.get('StartLine')
                    end_line = row.get('EndLine')
                    start_column = row.get('StartColumn')
                    end_column = row.get('EndColumn')
                    author = row.get('Author')
                    message = row.get('Message')
                    date = row.get('Date', date_value)
                    email = row.get('Email')
                    fingerprint = row.get('Fingerprint')
                    insert_data = [build_url, rule_id, commit, file_name, secret, match, start_line, end_line,
                                    start_column, end_column, author, message, date, email, fingerprint,
                                    date_value]
                    insert_query = postgres_utils.get_insert_query(cursor,
                                    constants.AR_GITLEAKS_METRICS, insert_data)
                    cursor.execute(insert_query)
                except Exception as ex:
                    all_rows_inserted = False
                    message = 'Error while inserting row {}: {}'.format(row, str(ex))
                    log.exception(message)
            if all_rows_inserted:
                message = 'Successfully persisted gitleak metrics into the database.'
                log.info(message)
                return message, HTTPStatus.OK
            else:
                log.error('''Partially persisted gitleak metrics into the database.
                            (Note : some rows may not have been inserted)''')
                return 'Gitleak metrics were Partially inserted', HTTPStatus.BAD_REQUEST
    except Exception as ex:
        message = 'Error while persisting gitleak metrics data into the database: {}'.format(str(ex))
        log.exception(message)
        return 'Error while persisting gitleak metrics data', HTTPStatus.BAD_REQUEST


def persist_into_psql_github_users(github_users: dict, postgres_server: str, postgres_user: str,
                                   postgres_token: str, postgres_db: str) -> None:
    """
    This function is to persist github users information in the database
    :param github_repos: github users data
    :param node_usage: nodes and its utilization
    :param postgres_server: postgres server name
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    """
    log.info('Persisting github users report into the database')
    try:
        with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                postgres_db) as db_obj:
            conn, cursor = db_obj
            conn.autocommit = True

            truncate_query = 'TRUNCATE TABLE {};'.format(constants.GITHUB_USERS)
            cursor.execute(truncate_query)
            log.info('Old Repositories Users Data Discarded')

            for row in github_users:
                try:
                    user_id = row.get('id')
                    login = row.get('login')
                    email = row.get('email')
                    role = row.get('role')
                    repo_alias = row.get('repo_alias')
                    is_suspended = row.get('suspended?')
                    last_logged_ip = row.get('last_logged_ip')
                    repositories = row.get('repos')
                    ssh_keys = row.get('ssh_keys')
                    org_memberships = row.get('org_memberships')
                    is_dormant = row.get('dormant?')
                    raw_login = row.get('raw_login')
                    is_2fa_enabled = row.get('2fa_enabled?')
                    created_at = datetime.strptime(row.get('created_at')[:-4], constants.DATE_FORMAT)
                    try:
                        last_active = datetime.strptime(row.get('last_active')[:-4], constants.DATE_FORMAT)
                    except Exception:
                        last_active = None

                    insert_data = [user_id, login, email, role, repo_alias, is_suspended, last_logged_ip,
                                    repositories, ssh_keys, org_memberships, is_dormant, last_active,
                                    raw_login, is_2fa_enabled, created_at]
                    insert_query = postgres_utils.get_insert_query(cursor, constants.GITHUB_USERS, insert_data)
                    cursor.execute(insert_query)

                except Exception as ex:
                    log.error('Error while persisting github user report into the database: {}'.format(str(ex)))
        log.info('Successfully persisted github users data into the database')
    except Exception as ex:
        message = 'Error while persisting github users into the database: {}'.format(str(ex))
        log.error(message)
        # raise MetricsError(message)


def persist_into_psql_github_users_trends(postgres_server: str, postgres_user: str,
                                   postgres_token: str, postgres_db: str) -> None:
    """
    This function is to persist github users trends information in the database
    :param postgres_server: postgres server name
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    """
    log.info('Persisting github users trends report into the database')
    try:
        with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                postgres_db) as db_obj:
            conn, cursor = db_obj
            conn.autocommit = True

            select_data = ["count({})".format(constants.SELECT_ALL)]
            select_query = postgres_utils.get_select_query(cursor, constants.GITHUB_USERS, select_data)
            cursor.execute(select_query)
            github_users_count = cursor.fetchone()[0]

            utc_today = datetime.utcnow().date()

            select_query = f"""SELECT github_users_count, time
                               FROM {constants.GITHUB_USERS_TRENDS}
                               WHERE Date(time)='{utc_today}'
                               ORDER BY time DESC
                               LIMIT 1;"""
            cursor.execute(select_query)
            current_entry = cursor.fetchone()

            if current_entry is not None:
                # An entry already exists for today's date, and existing Github users count and
                # current count doesn't match then Insert a new entry
                github_users_count_trend = current_entry[0]

                if github_users_count_trend != github_users_count:
                    utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)
                    insert_data = [github_users_count, utc_time]
                    insert_query = postgres_utils.get_insert_query(cursor, constants.GITHUB_USERS_TRENDS, insert_data)
                    cursor.execute(insert_query)
                else:
                    log.info('github users count has not changed')
            else:
                # No entry exists for today's date, insert a new row with count and time
                utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)
                insert_data = [github_users_count, utc_time]
                insert_query = postgres_utils.get_insert_query(cursor, constants.GITHUB_USERS_TRENDS, insert_data)
                cursor.execute(insert_query)
                log.info('Successfully inserted into github users trends table')

            log.info('Successfully persisted github users data into the database')
    except Exception as ex:
        message = 'Error while persisting github users trends into the database: {}'.format(str(ex))
        log.error(message)
        # raise MetricsError(message)


def persist_into_psql_github_repositories(github_repos: dict, postgres_server: str, postgres_user: str,
                                          postgres_token: str, postgres_db: str) -> None:
    """
    This function is to persist github repositories information in the database
    :param github_repos: github repositories data
    :param node_usage: nodes and its utilization
    :param postgres_server: postgres server name
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    """
    log.info('Persisting github repositories report into the database')
    try:
        with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                postgres_db) as db_obj:
            conn, cursor = db_obj
            conn.autocommit = True

            truncate_query = 'TRUNCATE TABLE {};'.format(constants.GITHUB_REPOS)
            cursor.execute(truncate_query)
            log.info('Old Repositories Data Discarded')

            for row in github_repos:
                try:
                    repo_id = row.get('id')
                    repo_name = row.get('name')
                    owner_id = row.get('owner_id')
                    owner_type = row.get('owner_type')
                    owner_name = row.get('owner_name')
                    visibility = row.get('visibility')
                    readable_size = row.get('readable_size')
                    raw_size = row.get('raw_size')
                    collaborators = row.get('collaborators')
                    repo_alias = row.get('repo_alias')
                    is_forked = row.get('fork?')
                    is_deleted = row.get('deleted?')
                    created_at = datetime.strptime(row.get('created_at')[:-4], constants.DATE_FORMAT)

                    insert_data = [repo_id, repo_name, owner_id, owner_type, owner_name,
                                visibility, readable_size, raw_size, collaborators, repo_alias,
                                is_forked, is_deleted, created_at]
                    insert_query = postgres_utils.get_insert_query(cursor, constants.GITHUB_REPOS,
                                                                insert_data)
                    cursor.execute(insert_query)
                except Exception as ex:
                    log.error('Error while persisting github repos report into the database: {}'.format(str(ex)))
        log.info('Successfully persisted github repos data into the database')
    except Exception as ex:
        message = 'Error while persisting github repos into the database: {}'.format(str(ex))
        log.error(message)
        # raise MetricsError(message)


def persist_into_psql_github_repositories_trends(postgres_server: str, postgres_user: str,
                                   postgres_token: str, postgres_db: str) -> None:
    """
    This function is to persist github repositories trends information in the database
    :param postgres_server: postgres server name
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    """
    log.info('Persisting github repositories trends report into the database')
    try:
        with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                postgres_db) as db_obj:
            conn, cursor = db_obj
            conn.autocommit = True

            select_data = ["count({})".format(constants.SELECT_ALL)]
            select_query = postgres_utils.get_select_query(cursor, constants.GITHUB_REPOS, select_data)
            cursor.execute(select_query)
            github_repos_count = cursor.fetchone()[0]

            utc_today = datetime.utcnow().date()

            select_query = f"""SELECT github_repos_count, time
                               FROM {constants.GITHUB_REPOS_TRENDS}
                               WHERE Date(time)='{utc_today}'
                               ORDER BY time DESC
                               LIMIT 1;"""
            cursor.execute(select_query)
            current_entry = cursor.fetchone()

            if current_entry is not None:
                # An entry already exists for today's date, and existing Github repo count and
                # current count doesn't match then Insert a new entry
                github_repos_count_trend = current_entry[0]

                if github_repos_count_trend != github_repos_count:
                    utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)
                    insert_data = [github_repos_count, utc_time]
                    insert_query = postgres_utils.get_insert_query(cursor, constants.GITHUB_REPOS_TRENDS, insert_data)
                    cursor.execute(insert_query)
                else:
                    log.info('Github Repo count has not changed')
            else:
                # No entry exists for today's date, insert a new row with count and time
                utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)
                insert_data = [github_repos_count, utc_time]
                insert_query = postgres_utils.get_insert_query(cursor, constants.GITHUB_REPOS_TRENDS, insert_data)
                cursor.execute(insert_query)
                log.info('Successfully inserted into github repos trends table')

            log.info('Successfully persisted github repos trends data into the database')
    except Exception as ex:
        message = 'Error while persisting github repos trends into the database: {}'.format(str(ex))
        log.error(message)
        # raise MetricsError(message)


def persist_gitleak_scan_info(build_url, gitleaks_status, postgres_server: str,
                    postgres_user: str, postgres_token: str, postgres_db: str) -> None:
    """
    This function is to persist gitleak status data into the postgres database
    :param build_url: jenkins build url
    :param gitleaks_status: SUCCESS or FAILED or ERROR status
    :param postgres_server: postgres server name
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    """
    log.info('Persisting gitleak enablement status into the postgres database')

    try:
        with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                postgres_db) as db_obj:
            conn, cursor = db_obj
            conn.autocommit = True

            utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)
            insert_data = [build_url, gitleaks_status, utc_time]
            insert_query = postgres_utils.get_insert_query(cursor,
                                            constants.AR_GITLEAKS_SCAN_INFO, insert_data)
            cursor.execute(insert_query)

    except Exception as ex:
        message = 'Error while inserting data {}'.format(str(ex))
        log.exception(message)
        # raise MetricsError(message)


def persist_playbook_metrics_postgres(playbook_enforcement_dict: list, postgres_server: str,
                    postgres_user: str, postgres_token: str, postgres_db: str) -> Tuple[str, int]:
    """
    This function is to persist playbook enforcement metrics into the postgres database
    :param playbook_enforcement_dict: gathered playbook enforcement metrics
    :param postgres_server: postgres server name
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    """
    log.info('Persisting playbook enforcement metrics into the postgres database')

    try:
        with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                postgres_db) as db_obj:
            conn, cursor = db_obj
            conn.autocommit = True

            for row in playbook_enforcement_dict:
                try:
                    inventory_hostname = row.get('inventory_hostname')
                    managing_team = row.get('managing_team')
                    os = row.get('OS')
                    os_version = row.get('OS_version')
                    os_virtualization_type = row.get('OS_virt_type')
                    motd_changed = row.get('motd_changed')
                    motd_checked_time = row.get('motd_checked_time')
                    motd_modified_time = row.get('motd_modified_time')
                    dns_changed = row.get('dns_changed')
                    dns_checked_time = row.get('dns_checked_time')
                    dns_modified_time = row.get('dns_modified_time')
                    network_manager_changed = row.get('network_manager_changed')
                    network_manager_checked_time = row.get('network_manager_checked_time')
                    network_manager_modified_time = row.get('network_manager_modified_time')
                    snmp_config_changed = row.get('snmp_conf_changed')
                    snmp_checked_time = row.get('snmp_checked_time')
                    snmp_modified_time = row.get('snmp_modified_time')
                    mdatp_package_changed = row.get('mdatp_package_changed')
                    mdatp_package_checked_time = row.get('mdatp_package_checked_time')
                    mdatp_package_modified_time = row.get('mdatp_package_modified_time')
                    mdatp_config_changed = row.get('mdatp_config_changed')
                    mdatp_config_checked_time = row.get('mdatp_config_checked_time')
                    mdatp_config_modified_time = row.get('mdatp_config_modified_time')

                    select_data = constants.SELECT_ALL
                    where_data = {'inventory_hostname' : inventory_hostname}
                    select_query = postgres_utils.get_select_query(cursor,
                                    constants.GCM_PLAYBOOK_ENFORCEMENT,select_data, where_data)
                    cursor.execute(select_query)
                    existing_row = cursor.fetchone()
                    if existing_row:
                        update_data = dict()

                        if motd_changed == False:
                            update_data['motd_checked_time'] = motd_checked_time
                        elif motd_changed == True:
                            update_data['motd_modified_time'] = motd_modified_time
                        elif motd_changed == 'skipped':
                            motd_changed = False
                        update_data['motd_changed'] = motd_changed

                        if dns_changed == False:
                            update_data['dns_checked_time'] = dns_checked_time
                        elif dns_changed == True:
                            update_data['dns_modified_time'] = dns_modified_time
                        elif dns_changed == 'skipped':
                            dns_changed = False
                        update_data['dns_changed'] = dns_changed

                        if network_manager_changed == False:
                            update_data['network_manager_checked_time'] = network_manager_checked_time
                        elif network_manager_changed == True:
                            update_data['network_manager_modified_time'] = network_manager_modified_time
                        elif network_manager_changed == 'skipped':
                            network_manager_changed = False
                        update_data['network_manager_changed'] = network_manager_changed

                        if snmp_config_changed == False:
                            update_data['snmp_checked_time'] = snmp_checked_time
                        elif snmp_config_changed == True:
                            update_data['snmp_modified_time'] = snmp_modified_time
                        elif snmp_config_changed == 'skipped':
                            snmp_config_changed = False
                        update_data['snmp_config_changed'] = snmp_config_changed

                        if mdatp_package_changed == False:
                            update_data['mdatp_package_checked_time'] = mdatp_package_checked_time
                        elif mdatp_package_changed == True:
                            update_data['mdatp_package_modified_time'] = mdatp_package_modified_time
                        elif mdatp_package_changed == 'skipped':
                            mdatp_package_changed = False
                        update_data['mdatp_package_changed'] = mdatp_package_changed


                        if mdatp_config_changed == False:
                            update_data['mdatp_config_checked_time'] = mdatp_config_checked_time
                        elif mdatp_config_changed == True:
                            update_data['mdatp_config_modified_time'] = mdatp_config_modified_time
                        elif mdatp_config_changed == 'skipped':
                            mdatp_config_changed = False
                        update_data['mdatp_config_changed'] = mdatp_config_changed

                        update_query = postgres_utils.get_update_query(cursor,
                                        constants.GCM_PLAYBOOK_ENFORCEMENT, update_data, where_data)
                        cursor.execute(update_query)
                    else:
                        if motd_changed == 'skipped':
                            motd_changed = False
                            motd_checked_time = None
                            motd_modified_time = None
                        elif motd_changed == False:
                            motd_modified_time = None
                        elif motd_changed == True:
                            motd_checked_time = None
                        if dns_changed == 'skipped':
                            dns_changed = False
                            dns_checked_time = None
                            dns_modified_time = None
                        elif dns_changed == False:
                            dns_modified_time = None
                        elif dns_changed == True:
                            dns_checked_time = None
                        if network_manager_changed == 'skipped':
                            network_manager_changed = False
                            network_manager_checked_time = None
                            network_manager_modified_time = None
                        elif network_manager_changed == False:
                            network_manager_modified_time = None
                        elif network_manager_changed == True:
                            network_manager_checked_time = None
                        if snmp_config_changed == 'skipped':
                            snmp_config_changed = False
                            snmp_checked_time = None
                            snmp_modified_time = None
                        elif snmp_config_changed == False:
                            snmp_modified_time = None
                        elif snmp_config_changed == True:
                            snmp_checked_time = None
                        if mdatp_package_changed == 'skipped':
                            mdatp_package_changed = False
                            mdatp_package_checked_time = None
                            mdatp_package_modified_time = None
                        elif mdatp_package_changed == False:
                            mdatp_package_modified_time = None
                        elif mdatp_package_changed == True:
                            mdatp_package_checked_time = None
                        if mdatp_config_changed == 'skipped':
                            mdatp_config_changed = False
                            mdatp_config_checked_time = None
                            mdatp_config_modified_time = None
                        elif mdatp_config_changed == False:
                            mdatp_config_modified_time = None
                        elif mdatp_config_changed == True:
                            mdatp_config_checked_time = None

                        insert_data = [inventory_hostname, managing_team, os, os_version,
                                os_virtualization_type, motd_changed, motd_checked_time,
                                motd_modified_time, dns_changed, dns_checked_time,
                                dns_modified_time, network_manager_changed,
                                network_manager_checked_time, network_manager_modified_time,
                                snmp_config_changed, snmp_checked_time, snmp_modified_time,
                                mdatp_package_changed, mdatp_package_checked_time,
                                mdatp_package_modified_time,mdatp_config_changed,
                                mdatp_config_checked_time, mdatp_config_modified_time]
                        insert_query = postgres_utils.get_insert_query(cursor,
                                                                constants.GCM_PLAYBOOK_ENFORCEMENT,
                                                                insert_data)
                        cursor.execute(insert_query)
                except Exception as ex:
                    message = 'Error while inserting row {}: {}'.format(row, str(ex))
                    log.exception(message)

            message = 'Ansible playbook metrics Inserted Successfully'
            return message , HTTPStatus.OK
    except Exception as ex:
        message = 'Error while persisting ansible playbook metrics into database: {}'.format(str(ex))
        log.exception(message)
        return message , HTTPStatus.BAD_REQUEST


def persist_playbook_metrics_trends_postgres(postgres_server: str,
                    postgres_user: str, postgres_token: str, postgres_db: str) -> Tuple[str, int]:
    """
    This function is to persist playbook metrics trends into the postgres database
    :param gitleak_metrics: gathered playbook metrics trends
    :param postgres_server: postgres server name
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    """
    log.info('Persisting playbook metrics trends into the postgres database')

    try:
        with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                postgres_db) as db_obj:
            conn, cursor = db_obj
            conn.autocommit = True

            def execute_select_query(where_data):
                select_data = [constants.COUNT_ALL]
                select_query = postgres_utils.get_select_query(cursor,
                                constants.GCM_PLAYBOOK_ENFORCEMENT, select_data, where_data)
                cursor.execute(select_query)
                return cursor.fetchone()[0]

            motd_changed_count = execute_select_query({'motd_changed': True})
            dns_changed_count = execute_select_query({'dns_changed': True})
            network_manager_changed_count = execute_select_query({'network_manager_changed': True})
            snmp_changed_count = execute_select_query({'snmp_config_changed': True})
            mdatp_package_changed_count = execute_select_query({'mdatp_package_changed': True})
            mdatp_config_changed_count = execute_select_query({'mdatp_config_changed': True})

            utc_time = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)
            insert_data = [motd_changed_count, dns_changed_count,
                            network_manager_changed_count, snmp_changed_count,
                            mdatp_package_changed_count, mdatp_config_changed_count, utc_time]
            insert_query = postgres_utils.get_insert_query(cursor,
                        constants.GCM_PLAYBOOK_ENFORCEMENT_TRENDS, insert_data)
            cursor.execute(insert_query)
            message = 'Successfully inserted Ansible Playbook Enforcement Trends data'
            return message , HTTPStatus.OK

    except Exception as ex:
        message = 'Error while connecting/Inserting data to Database: {}'.format(str(ex))
        log.exception(message)
        return message, HTTPStatus.BAD_REQUEST
		
def persist_invalid_branch_metrics_postgres(invalid_branch_dict: list, postgres_server: str,
                                    postgres_user: str, postgres_token: str, postgres_db: str) -> tuple:
    """
    This function is to persist Invalid branch protection data into the postgres database
    :param invalid_branch_dict: gathered branch protection metrics
    :param postgres_server: postgres server name
    :param postgres_user: postgres username
    :param postgres_token: postgres token
    :param postgres_db: postgres database
    """
    log.info('Persisting Invalid branch protection data into the postgres database')

    try:
        with postgres_utils.connect_to_database(postgres_server, postgres_user, postgres_token,
                                                postgres_db) as db_obj:
            conn, cursor = db_obj
            conn.autocommit = True

            inserted_date = updated_date = datetime.now(tz=timezone.utc).strftime(constants.DATE_FORMAT)
            time = datetime.now(tz=timezone.utc).strftime(constants.TIME_FORMAT)
            for row in invalid_branch_dict:
                try:
                    organization = row.get('Organization')
                    repository = row.get('Repository')
                    branch = row.get('Branch')
                    status = row.get('Comment')

                    select_data = [constants.SELECT_ALL]
                    where_data = {'organization': organization, 'repository': repository,
                                'branch': branch, 'status': status}
                    select_query = postgres_utils.get_select_query(cursor,
                                    constants.INVALID_GIT_BRANCH_PROTECTION, select_data, where_data)
                    cursor.execute(select_query)
                    data = cursor.fetchone()
                    if not data:
                        insert_data = [organization, repository, branch, status, inserted_date, updated_date]
                        insert_query = postgres_utils.get_insert_query(cursor,
                                        constants.INVALID_GIT_BRANCH_PROTECTION, insert_data)
                        cursor.execute(insert_query)
                    else:
                        update_data = {'updated_date': updated_date}
                        where_data = {'organization': organization, 'repository': repository,
                                    'branch': branch, 'status': status}
                        update_query = postgres_utils.get_update_query(cursor,
                                    constants.INVALID_GIT_BRANCH_PROTECTION, update_data, where_data)
                        cursor.execute(update_query)

                except Exception as ex:
                    message = 'Error while inserting row {}: {}'.format(row, str(ex))
                    log.exception(message)

            select_data = [constants.COUNT_ALL]
            where_data = {'status': 'enforce_admins not enabled', 'updated_date': updated_date}
            select_query = postgres_utils.get_select_query(cursor,
                            constants.INVALID_GIT_BRANCH_PROTECTION, select_data, where_data)
            cursor.execute(select_query)
            enforce_admins_not_enabled_count = cursor.fetchone()[0]
            where_data = {'status': 'Branch not protected', 'updated_date': updated_date}
            select_query = postgres_utils.get_select_query(cursor,
                            constants.INVALID_GIT_BRANCH_PROTECTION, select_data, where_data)
            cursor.execute(select_query)
            branch_not_protected_count = cursor.fetchone()[0]

            insert_data = [branch_not_protected_count, enforce_admins_not_enabled_count, time]
            insert_query = postgres_utils.get_insert_query(cursor,
                            constants.INVALID_GIT_BRANCH_PROTECTION_TRENDS, insert_data)
            cursor.execute(insert_query)
            return "Invalid branch protection data were successfully inserted", HTTPStatus.OK
    except Exception as ex:
        message = 'Error while persisting Invalid branch protection data into the database: {}'.format(str(ex))
        log.exception(message)
        return message, HTTPStatus.INTERNAL_SERVER_ERROR