import logging
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Define the function to list S3 objects using boto3
def list_s3_objects(**kwargs):
    bucket_name = kwargs["bucket_name"]
    session = boto3.Session()
    s3_client = session.client("s3", region_name="ap-southeast-1")

    # Set up a logger for the task
    logger = logging.getLogger("airflow.task")

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            logger.info(f"Objects in bucket {bucket_name}:")
            for obj in response["Contents"]:
                logger.info(obj["Key"])
        else:
            logger.info(f"No objects found in bucket {bucket_name}")
    except Exception as e:
        logger.error(f"Error listing objects in bucket {bucket_name}: {e}")

# Define default arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

your_bucket_name = "staging-autokey-flowacc-ocr-result.flowaccount.dev"

# Define the DAG
dag = DAG(
    "list_s3_objects_boto3_dag",
    default_args=default_args,
    description="A simple DAG to list objects in an S3 bucket using boto3",
    schedule_interval=None,  # Run on demand
)

# Define the PythonOperator
list_objects_task = PythonOperator(
    task_id="list_s3_objects_task",
    python_callable=list_s3_objects,
    op_kwargs={"bucket_name": your_bucket_name},  # Replace with your bucket name
    dag=dag,
)

list_objects_task
