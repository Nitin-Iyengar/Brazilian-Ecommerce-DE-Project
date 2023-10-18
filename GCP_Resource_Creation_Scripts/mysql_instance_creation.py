from googleapiclient import discovery
from google.oauth2 import service_account

SERVICE_ACCOUNT_KEY_FILE = 'D:/Service Accounts/Cloud SQL/sql_admin.json'


# Authenticate with GCP using the service account JSON key file
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_KEY_FILE,
    scopes=["https://www.googleapis.com/auth/sqlservice.admin"]
)

PROJECT_ID = 'my-ecommerce-de-project-97'
INSTANCE_NAME = 'de-project-dataset099'
ROOT_USER = 'root'
ROOT_PASSWORD = '######'


# Create a Google Cloud SQL instance
sqladmin = discovery.build('sqladmin', 'v1beta4', credentials=credentials)
request = sqladmin.instances().insert(
    project=PROJECT_ID,
    body={
        "name": INSTANCE_NAME,
        "settings": {
            "tier": "db-n1-standard-1",  # Specify the instance type
            "region": "us-central1",
            "backupConfiguration": {
                "enabled": True
            },
            "databaseFlags": [{
                "name": "max_allowed_packet",
                "value": "1073741824"
            }],
            "ipConfiguration": {
                "ipv4Enabled": True,
                "privateNetwork": "my-vpc-network",
                # "requireSsl": True
            },
            "databaseVersion": "MYSQL_8_0",
            "deletionProtection": False,
            "rootPassword": ROOT_PASSWORD,
            "users": [{
                "name": ROOT_USER,
                "password": ROOT_PASSWORD
            }]
        }
    }
)
response = request.execute()

print(f"Instance created: {INSTANCE_NAME}")