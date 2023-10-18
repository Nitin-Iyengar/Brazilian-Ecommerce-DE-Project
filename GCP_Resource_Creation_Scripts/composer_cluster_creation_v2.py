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
        'nodeConfig': {
            "serviceAccount": "composer-sa-de-project@my-ecommerce-de-project-97.iam.gserviceaccount.com",
            "network": "projects/my-ecommerce-de-project-97/global/networks/my-vpc-network",
            "subnetwork": "projects/my-ecommerce-de-project-97/regions/us-central1/subnetworks/vpc-test",
        },
        'softwareConfig': {
            'imageVersion': "composer-2.4.5-airflow-2.5.3"
        },
        "environmentSize": "ENVIRONMENT_SIZE_SMALL"
    }
}

# Create the environment
parent = f'projects/{project_id}/locations/{location}'
request = composer.projects().locations().environments().create(
    parent=parent, body=environment
)
response = request.execute()

print(f'Environment created: {response}')
