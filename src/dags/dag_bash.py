from airflow.operators.bash import BashOperator
from datetime import date, datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

default_args = {
    'start_date': datetime(2024, 2, 1),
    'owner': 'airflow'
}

dag = DAG("dag_bash",
          schedule_interval=None,
          default_args=default_args
         )

date = datetime.date

dm_users = BashOperator(
    task_id='dm_user',
    bash_command=f'/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/dm_users.py {date}',
        retries=3,
        dag=dag
)

events_zone = BashOperator(
    task_id='events_zone',
    bash_command=f'/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/events_zone.py {date}',
        retries=3,
        dag=dag
)

recommendation_users = BashOperator(
    task_id='recommendation_users',
    bash_command=f'/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /lessons/recommendation_users.py {date}',
        retries=3,
        dag=dag
)

dm_users