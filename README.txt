This project presents a cloud-native parallel image processing pipeline built on Google Cloud, PySpark and Flask.

GCLOUD SDK:
1. Download the setup and install: https://cloud.google.com/sdk/docs/install
2. Login to your account: gcloud auth login
3. Initialize your default configuration: gcloud init
4. Set the active project: gcloud config set project PROJECT_ID


1. CREATE STORAGE BUCKETS:
gsutil mb -l us-central1 gs://input-bucket-ecc
gsutil mb -l us-central1 gs://output-bucket-ecc

2. API's ENABLED:
1. Google Cloud Dataproc API
2. Google Cloud Compute Engine API
3. Google Cloud Storage API
4. Cloud Logging and Cloud Monitoring APIs
5. App Engine Admin API (or Cloud Run API)


3. Upload Image Dataset to the Input Bucket:
gsutil -m cp -r {local_path}\caltech101 gs://input-bucket-ecc/

4. Upload the PySpark Script to GCS:
gsutil cp {local_path}\image_processing_v2.py gs://input-bucket-ecc/scripts/

5. New Cluster with component gateway:
gcloud dataproc clusters create new-cluster2 \
  --region=us-central1 \
  --num-workers=2 \
  --worker-machine-type=n1-standard-2 \
  --master-machine-type=n1-standard-2 \
  --image-version=2.0 \
  --enable-component-gateway \
  --initialization-actions=gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh \
  --metadata=PIP=pip3,PIP_PACKAGES="google-cloud-storage" \
  --master-boot-disk-size=50GB \
  --worker-boot-disk-size=50GB


6. Install the requirements:
pip install -r requirements.txt

7. Run Application: python app.



NOTE: CHANGE THE "Configuration" VARIABLES IN "app.py" TO YOUR ENVIRONMENT.