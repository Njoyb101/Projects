from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.http_sensor import HttpSensor
# from airflow.operators.empty import EmptyOperator # only for testing
from datetime import timedelta
import requests
from airflow import settings
from airflow.models import Connection
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator




orders_file = 'orders.csv'

dag_customer360 = DAG(
    dag_id= 'customer360',
    start_date= days_ago(1)
)

# S3 File : https://airflow-prj-s3.s3.ap-south-1.amazonaws.com/orders.csv
# orders_s3.host = airflow-prj-s3.s3.ap-south-1.amazonaws.com


# Checking for orders.csv file availability in S3
sensor = HttpSensor(
    task_id = 'watch_for_orders',
    http_conn_id = 'orders_s3', # UI ==> Admin/Connections/http connection to S3 
    endpoint = '/orders.csv',
    response_check = lambda response : response.status_code == 200 ,
    retries = 12,
    retry_delay = timedelta( minutes = 5 ),
    dag = dag_customer360
)
# =======================================================================================================

# Download the orders.csv from  S3 to EMR Edge node 
def get_order_url():
    session = settings.Session()
    connection = session.query(Connection).filter(Connection.conn_id == 'orders_s3').first()
    return f'{connection.schema}://{connection.host}/{orders_file}'

download_order_cmd = f'rm -rf airflow_orders && mkdir airflow_orders && cd airflow_orders && wget {get_order_url()} && echo download_complete'

s3_to_edge_node = SSHOperator (
    task_id = 'download_orders',
    ssh_conn_id = 's3ToEdge', # to be created one https connection in UI/Admin/Connection/create # ec2-13-234-75-116.ap-south-1.compute.amazonaws.com
    command = download_order_cmd , 
    # command = 'echo hello' , 
    dag = dag_customer360
)

# =======================================================================================================

# Sqoop is fetching the customers info from AWS RDS MySQL and putting it in EMR Hive table 
def fetch_customer_info_cmd() :
    hive_drop = "hive -e 'DROP TABLE airflow.customers'"
    hive_rm_dir = "hdfs dfs -rm -R -f customers"
    sqoop_import = """sqoop import \
    --connect jdbc:mysql://dbcustomers.cq9ssrm5mida.ap-south-1.rds.amazonaws.com:3306/dbCustomers \
    --username root \
    --password AirFloW45#9oQ18J& \
    --table customers \
    --hive-import \
    --create-hive-table \
    --hive-table airflow.customers"""
    return f'{hive_drop} && {hive_rm_dir} && {sqoop_import}'

import_crm = SSHOperator (
    task_id = 'import_crm',
    ssh_conn_id = 's3ToEdge', # to be created one https connection in UI/Admin/Connection/create # ec2-13-234-75-116.ap-south-1.compute.amazonaws.com 
    command = fetch_customer_info_cmd() , 
    dag = dag_customer360
)

# =======================================================================================================

# Upload the Orders to  HDFS of the EMR  from Edge Node
upload_orders_to_hdfs = SSHOperator (
    task_id = 'upload_orders_to_hdfs',
    ssh_conn_id = 's3ToEdge', # to be created one https connection in UI/Admin/Connection/create # ec2-13-234-75-116.ap-south-1.compute.amazonaws.com 
    command = 'hdfs dfs -rm -R -f airflow_input && hdfs dfs -mkdir -p airflow_input && hdfs dfs -put ./airflow_pipeline/orders.csv airflow_input', 
    dag = dag_customer360
)

# =======================================================================================================

# Spark Program to filter out the 'CLOSED' orders

# input : airflow_input/orders.csv
# output : airflow_output/_SUCCESS
# output : airflow_output/part-00000-80

def get_closed_orders():
    spark_version_set_up_cmd = "export SPARK_MAJOR_VERSION=2"
    clean_output_path_cmd = "hdfs dfs -rm -R -f airflow_output"
    spark_prgm_filter_closed_orders_cmd = "spark-submit --class DataFramesExample sparkbundle.jar airflow_input/orders.csv airflow_output"
    return f'{spark_version_set_up_cmd} && {clean_output_path_cmd} && {spark_prgm_filter_closed_orders_cmd}'

filter_closed_orders = SSHOperator (
    task_id = 'filter_closed_orders',
    ssh_conn_id = 's3ToEdge', 
    command = get_closed_orders(), 
    dag = dag_customer360
)

# =======================================================================================================


# Create Hive table from  Spark output
def create_order_hive_table_cmd():
    create_table_hive_cmd = """ hive -e "CREATE external table if not exists airflow.orders(order_id INT, order_date STRING, customer_id INT, status STRING) ROW FORMAT delimited fields terminated by ',' stored as textfile location 'user/hadoop/airflow_output' " """

create_order_hive_table = SSHOperator (
    task_id = 'create_order_hive_table',
    ssh_conn_id = 's3ToEdge', 
    command = create_order_hive_table_cmd(), 
    dag = dag_customer360
) 

# =======================================================================================================

# Uploading  the Hive table data to HDFS for fast querying

def upload_in_hbase_cmd():
    create_hbase_table = 'hive -e "create table if not exists airflow.airflow_hbase(customer_id int, customer_fname string, customer_lname string, order_id int, order_date string) STORED BY \'org.apache.hadoop.hive.hbase.HBaseStorageHandler\' with SERDEPROPERTIES(\'jbase.columns.mapping\'= \':key,personal:customer_fname, personal:customer_lname, personal:ordr_id, personal:order_date\')"'

    insert_data_into_hbase_with_join_customers_orders = 'hive -e "insert overwrite table airflow.airflow_hbase select c.customer_id, c.customer_fname, c.customer_lname, o.order_id,o.order_date from airflow.customers c join airflow.orders o ON (c.customer_id = o.customer_id)"'

    return f'{create_hbase_table} && {insert_data_into_hbase_with_join_customers_orders}'

upload_in_hbase = SSHOperator (
    task_id = 'upload_in_hbase',
    ssh_conn_id = 's3ToEdge', 
    command = upload_in_hbase_cmd(), 
    dag = dag_customer360
)

# =======================================================================================================


# Slack Notification

# host :  hooks.slack.com/services
# connection ID : slack_webhook
# scheme : https

def slack_password():
    return 'sl*9g$5!mEK9^'

slack_notification_sucess = SlackWebhookOperator(
    task_id = 'slack_notification_sucess',
    http_conn_id = 'slack_webhook', 
    message = 'data uploaded successfully', 
    channel = '#customer-360 data upload',
    username = 'airflow',
    webhook_token = slack_password(),
    dag = dag_customer360
)

slack_notification_fail = SlackWebhookOperator(
    task_id = 'slack_notification_sucess',
    http_conn_id = 'slack_webhook', 
    message = 'data uploadfailed', 
    channel = '#customer-360 data upload',
    username = 'airflow',
    webhook_token = slack_password(),
    trigger_rule = 'all_failed'
    dag = dag_customer360
)

# =======================================================================================================



# Dummy Task to finish the DAG 

# dummy_task = EmptyOperator(
#     task_id="dummy_task",
#     dag = dag_customer360
# )

# =======================================================================================================



# DAG Task Sequence

sensor >> import_crm 

sensor >> s3_to_edge_node >> upload_orders_to_hdfs >> filter_closed_orders

[import_crm, filter_closed_orders] >> upload_in_hbase >> [slack_notification_sucess, slack_notification_fail]

# =======================================================================================================