import platform

pname = platform.system()
PARENT_DIR = "/Users/blevine/goonalytics" if pname == 'Darwin' else "/goonalytics"

GCLOUD_STORAGE_BUCKET = 'staging.empyrean-bridge-150804.appspot.com'
POST_SCHEMA_LOCATION = PARENT_DIR + "/resources/post-avro-schema.avsc"
THREAD_SCHEMA_LOCATION = PARENT_DIR + "/resources/thread-avro-schema.avsc"
GCLOUD_POST_TABLE = 'posts_raw'
GCLOUD_THREAD_TABLE = 'threads'
GCLOUD_PROJECT_NAME = 'empyrean-bridge-150804'
GCLOUD_DATASET_NAME = 'forums'
DATABASE_LOCATION = PARENT_DIR + '/resources/games_crawl.sqlite'
ELASTIC_LOCATION = 'http://ubentu.local:5601'

GCLOUD_CREDENTIAL_FILE = PARENT_DIR+'/resources/gcloud-cred.json'
