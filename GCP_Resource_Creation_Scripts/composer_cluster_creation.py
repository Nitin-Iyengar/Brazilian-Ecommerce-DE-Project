from googleapiclient import discovery
from google.oauth2 import service_account

# Load your service account credentials
SERVICE_ACCOUNT_KEY_FILE = "D:/Service Accounts/Composer/composer_admin.json"
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_KEY_FILE,
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Build the Composer API service
composer = discovery.build('composer', 'v1', credentials=credentials)

# Define your environment configuration
project_id = 'my-ecommerce-de-project-97'
location = 'us-central1'
environment_name = 'de-proj-airflow-env'

environment = {
    'name': f'projects/{project_id}/locations/{location}/environments/{environment_name}',
    'config': {
        'nodeCount': 3,
        'nodeConfig': {
            'machineType': f'projects/{project_id}/zones/us-central1-b/machineTypes/n1-standard-1',
            'diskSizeGb': 30,
            "serviceAccount": "composer-sa-de-project@my-ecommerce-de-project-97.iam.gserviceaccount.com"
        },
        'softwareConfig': {
            'schedulerCount': 1,
            'imageVersion': "composer-1.20.12-airflow-2.4.3"
        },
        'databaseConfig': {
            'machineType': 'db-n1-standard-2'
        },
        'webServerConfig': {
            'machineType': 'composer-n1-webserver-2'
        },
        'networkConfig': 'my-vpc-network',
    }
}

# Create the environment
parent = f'projects/{project_id}/locations/{location}'
request = composer.projects().locations().environments().create(
    parent=parent, body=environment
)
response = request.execute()

print(f'Environment created: {response}')
