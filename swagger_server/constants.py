PROJECT_RELEASE = '1.6'
ENV_NAME = 'APP_ENV'
FLO_NAME = 'dynaflo-metrics'
CLIENT_ID = 'AZURE_CLIENT_ID'
TENANT_ID = 'AZURE_TENANT_ID'
CLIENT_SECRET = 'AZURE_CLIENT_SECRET'
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DATE_FORMAT = '%Y-%m-%d'
HTTPS = 'https://'
QUERY = '''SELECT * FROM "{}" WHERE cid='{}';'''
JOB_URL_REGEX = r"^(http(s)?://[\w.-]+(:\d{4,5})?/)((job/[\w\s.%&()+–-]+/){1,})$"
BUILD_URL_REGEX = r"^(http(s)?://[\w.-]+(:\d{4,5})?)/((job/[\w\s.%&()+–-]+/){1,})(\d+)/$"
JIRA_REGEX = r"([A-Za-z]+\-\d+)"
NON_CICD = 'non-CICD'
CICD = 'CICD'
DEPLOY_ALL = 'deploy-all'
GROUP_DEPLOY = 'group-deploy'
PR_JOB = 'pull-request'

# other constants
STRING_PARAMETER = 'hudson.model.StringParameterValue'
TEXT_PARAMETER = 'hudson.model.TextParameterValue'
FOLDER_PROPERTIES = 'com.mig82.folders.properties.FolderProperties'
STRING_PROPERTIES = 'com.mig82.folders.properties.StringProperty'
BRANCH_SOURCE = 'jenkins.branch.BranchSource'
MULTI_BRANCH_PLUGIN = 'org.jenkinsci.plugins.pipeline.multibranch.defaults.DefaultsBinder'
CPS_SCM_PLUGIN = 'org.jenkinsci.plugins.workflow.cps.CpsScmFlowDefinition'
MULTI_BRANCH_PROJECT = 'org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject'

# config files
LOG_CONFIG_FILE = 'swagger_server/log.conf'
CONFIG_FILE = 'swagger_server/conf/{flo_env}/configuration.yml'
SCHEMA_FILE = 'swagger_server/schema/configuration_schema.yml'
BU_MAPPER_FILE = 'v1/BU-mapper.yml'
METRICS_FILE = 'v1/metrics.yml'
APPROVERS_FILE = 'v1/approvers.yml'

# postgres constants
POSTGRES_DB = 'dynaflo_metrics'
PIPELINE_TEMPLATES = 'pipeline_templates'
DYNAFLO_REQUESTS_PSQL = 'dynaflo_requests'
#AR - All Regions
AR_APPS = 'ar_applications_cicd'
AR_JENKINS_FAILURES = 'ar_jenkins_failures'
AR_DEV_TO_PROD = 'ar_dev_to_prod'
AR_APP_COUNT = 'ar_app_count'
AR_APP_COUNT_PIPELINE = 'ar_app_count_pipeline'
AR_SONARQUBE_TRENDS = 'ar_sonarqube_trends'
AR_TWISTLOCK_TRENDS = 'ar_twistlock_trends'
AR_CHECKMARX_TRENDS = 'ar_checkmarx_trends'
AR_PIPELINE_TRENDS = 'ar_pipeline_trends'
AR_NODE_USAGE = 'ar_nodes_usage'
AR_DYNAFLO_VIOLATIONS = 'ar_dynaflo_violations'
AR_APPS_NONCICD = 'ar_applications_noncicd'
AR_ALL_METRICS = 'ar_all_metrics'
AR_GITLEAKS_METRICS = 'ar_gitleaks_metrics'
AR_GITLEAKS_SCAN_INFO = 'ar_gitleaks_scan_info'
AR_GITLEAKS_ENABLEMENT_STATUS = 'ar_gitleaks_enablement_status'
GITHUB_USERS = 'github_users'
GITHUB_USERS_TRENDS = 'github_users_trends'
GITHUB_REPOS = 'github_repositories'
GITHUB_REPOS_TRENDS = 'github_repositories_trends'
AR_GITLEAKS_STATUS_TRENDS = 'ar_gitleaks_status_trends'
AR_APM_TRENDS = 'ar_apm_trends'
INVALID_GIT_BRANCH_PROTECTION = 'invalid_git_branch_protection'
INVALID_GIT_BRANCH_PROTECTION_TRENDS = 'invalid_git_branch_protection_trends'
GCM_PLAYBOOK_ENFORCEMENT = 'gcm_playbook_enforcements'
GCM_PLAYBOOK_ENFORCEMENT_TRENDS = 'gcm_playbook_enforcements_trends'

# postgres query constants
SELECT_ALL = '*'
COUNT_ALL = 'COUNT(*)'
COUNT_DISTINCT_REPO = 'COUNT(DISTINCT(business_unit, jenkins_region, organization, repository, app_name))'
COUNT_DISTINCT_BR = 'COUNT(DISTINCT(business_unit, jenkins_region, organization, repository, branch, app_name))'
COUNT_DISTINCT_APM = 'COUNT(DISTINCT(chear_id))'

# sonar metrics
SONAR_METRICS = (
    'alert_status',
    'new_blocker_violations',
    'new_critical_violations',
    'new_major_violations',
    'new_minor_violations',
    'new_info_violations',
    'blocker_violations',
    'critical_violations',
    'major_violations',
    'minor_violations',
    'info_violations',
    'coverage',
    'reliability_rating',
    'sqale_rating',
    'security_review_rating',
    'security_rating',
    'new_coverage',
    'tests',
    'test_failures',
    'test_errors',
    'test_success_density',
    'skipped_tests',
    'test_execution_time'
)
NA_SONAR_SERVER = 'https://sonar.chubbdigital.com'

# github constants
GIT_ORG_URL = 'https://github.chubb.com/api/v3/organizations'
CONTENT_URL = 'https://github.chubb.com/api/v3/repos/{}/{}/contents/{}?ref={}'
TREE_URL = 'https://github.chubb.com/api/v3/repos/{}/{}/git/trees/{}?recursive=1'
COMMITS_URL = 'https://github.chubb.com/api/v3/repos/{}/{}/commits'
COMMIT_DETAILS_URL = 'https://github.chubb.com/api/v3/repos/{}/{}/commits/{}'
PR_URL = 'https://github.chubb.com/api/v3/repos/{}/{}/pulls/{}'
COMMIT_SEARCH_URL = 'https://github.chubb.com/api/v3/search/commits?q=hash:{}'
REPORTS_URL = 'https://github.chubb.com/stafftools/reports/{}'
DIGITAL_DEVOPS_ORG = 'Digital-DevOps'
DYNAFLO_STORE = 'dynaflo-store'
PIPELINE_FOLDER = 'v1/pipeline/'
CUSTOM_FOLDER = 'v1/'
MASTER_BRANCH = 'master'
APPROVERS_YML = 'v1/approvers.yml'
PIPELINE_INFO_YML = 'v1/pipeline_info.yml'
TOKEN = 'token {}'
NOT_AVAILABLE = '-NA-'
MAX_REDIRECTS = 3
REDIRECT_STATUS_CODES = (
    301,
    302
)

# mailing constants
DEFAULT_SMTP_HOST = 'mail.chubb.com'
DEFAULT_MAIL_TO = 'Dynaflo_Support@chubb.com'
DEFAULT_MAIL_FROM = 'noreply-dynaflo-metrics@chubb.com'
DYNAFLO_SUBJECT = 'Dynaflo Request Failed in Prod'
MAIL_RECIPIENTS = 'Omprakash.Vangara@chubb.com,Nagaraju.Meruva@Chubb.com,Annapoorna.Shastry@chubb.com'

# messaging constants
CONFIG_SUCCESS_LOAD = 'Successfully loaded the configuration'
MSG_RECEIVED = 'Received request'
MSG_NO_JSON = 'Not a JSON content'
JSON_CONTENT = 'Content type is JSON'
MSG_NO_DATA = 'Data received is not of type dict'
MSG_FAILURE = 'Unable to process the webhook'
DASHBOARDS_MSG_FAILURE = 'Error while processing the payload'
MSG_POSTGRES_CONNECT_FAIL = 'Unable to connect to postgres DB'
MSG_POSTGRES_WRITE_FAIL = 'Failed to write to postgres DB'
MSG_DB_WRITE_SUCCESS = 'Successfully persisted dynaflo metrics into databases'
MSG_POSTGRES_INSERT_FAIL = 'Failed to insert data into postgres DB'
MSG_POSTGRES_UPDATE_FAIL = 'Failed to update data into postgres DB'
MSG_SENTRY_ERROR = 'Error while configuring sentry'
UNHEALTHY_SERVER = '{} env variable is not set, server is not healthy'
JENKINS_POST_BUILD_SUCCESSFUL ='Successfully processed jenkins post build data'

# Pipeline Type
PIPELINE_MANAGED = 'Pipeline Managed'
PIPELINE_CUSTOM = 'Pipeline Custom'
PIPELINE_SCM = 'Pipeline SCM'
PIPELINE_SCALABLE = 'Pipeline Scalable'
ONE_TEMPLATE = 'One Template'
UNCATEGORIZED = 'un-categorized'

# Grafana Constants
DASHBOARD_TITLE = '{} applications dashboard'
DASHBOARD_CREATE_MESSAGE = 'Creating application dashboard for: {}'

# Jira Constants
NA_JIRA_SERVER = 'https://jira.chubb.com'
FIELDS_KEY = 'fields'
LIBRARY_REPOS = (
    'dynaflo-custom-pipelines',
    'jenkins-global-library',
    'jenkins-global-config',
    'job-config-flo-templates'
)
DTO_PROJ_KEYS = (
    'DATAPLT',
    'FIND',
    'PARTNER',
    'PLATFORM',
    'PRODUCT',
    'RENEW'
)
