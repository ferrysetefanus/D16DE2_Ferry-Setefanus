# [START composer_hadoop_tutorial]
"""Example Airflow DAG that creates a Cloud Dataproc cluster, runs the Hadoop
wordcount example, and deletes the cluster.
This DAG relies on three Airflow variables
https://airflow.apache.org/concepts.html#variables
* gcp_project - Google Cloud Project to use for the Cloud Dataproc cluster.
* gce_zone - Google Compute Engine zone where Cloud Dataproc cluster should be
  created.
* gcs_bucket - Google Cloud Storage bucket to use for result of Hadoop job.
  See https://cloud.google.com/storage/docs/creating-buckets for creating a
  bucket.
"""
import datetime
import os
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.utils import trigger_rule

gcp_project = "idyllic-coder-400009"
gce_zone = "asia-southeast1-b"
gcs_bucket = "gs://us-central1-highcpu-11357220-bucket"
gce_region = "asia-southeast1"

# Output file for Cloud Dataproc job.
output_file = "gs://us-central1-highcpu-11357220-bucket/output"

# Arguments to pass to Cloud Dataproc job.
input_file = 'gs://pub/shakespeare/rose.txt'

# path to pyspark wordcount script to wordcount
pyspark_script = 'gs://us-central1-highcpu-11357220-bucket/scripts/wordcount_spark.py'
pyspark_args = [input_file, output_file]

dataproc_job = {
    "reference": {"project_id": gcp_project},
    "placement": {"cluster_name": "composer-pyspark-tutorial-cluster-{{ ds_nodash }}"},
    "pyspark_job": {
        "main_python_file_uri": pyspark_script,
        "args": pyspark_args,
    },
}

CLUSTER_CONFIG = {
    "master_config": {"num_instances": 1, "machine_type_uri": "e2-standard-2", "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32}},
    "worker_config": {"num_instances": 2, "machine_type_uri": "e2-standard-2", "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32}},
}

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())
default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': gcp_project
}
# [START composer_hadoop_schedule]
with models.DAG(
        'assignment16',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    # [END composer_hadoop_schedule]

    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name="composer-pyspark-tutorial-cluster-{{ ds_nodash }}",
        cluster_config=CLUSTER_CONFIG,
        region=gce_region,
        )
    
    # Run the Hadoop wordcount example installed on the Cloud Dataproc cluster
    # master node.
    run_dataproc_pyspark = DataprocSubmitJobOperator(
        task_id='run_dataproc_pyspark',
        job=dataproc_job,
        region=gce_region
        )
    
    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        region=gce_region,
        cluster_name='composer-pyspark-tutorial-cluster-{{ ds_nodash }}',
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)
    
    # [START composer_hadoop_steps]
    # Define DAG dependencies.
    create_dataproc_cluster >> run_dataproc_pyspark >> delete_dataproc_cluster
    # [END composer_hadoop_steps]
# [END composer_hadoop_tutorial]