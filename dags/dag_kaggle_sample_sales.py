from airflow                                                  import DAG
from airflow.models                                           import Variable
from airflow.providers.google.cloud.operators.cloud_functions import CloudFunctionInvokeFunctionOperator
from airflow.operators.python                                 import PythonOperator
from datetime                                                 import datetime
from pytz                                                     import timezone
## Bibliotecas desenvolvidas pelo time no diretorio modules ##
from modules.google_chat_notification                         import google_chat_notification

## FUNCOES ##

def get_airflow_env_vars():
    """
    Função centralizada para importação das variáveis de ambiente do Airflow.
    Retorna um dicionário com todas as variáveis necessárias.
    """
    environment_variables             = Variable.get('environment_variables', deserialize_json=True)
    fnc_kaggle_sample_sales_variables = Variable.get('fnc_kaggle_sample_sales_variables', deserialize_json=True)

    return {
        "project_id":   environment_variables['project_id'],
        "region":       environment_variables['region'],
        "gcp_conn_id":  environment_variables['gcp_conn_id'],
        "webhook_url":  environment_variables['webhook_url'],
        "function_id":  fnc_kaggle_sample_sales_variables['function_id'],
        "input_data":   fnc_kaggle_sample_sales_variables['input_data']
    }

def lib_google_chat_notification_error(context,webhook_url = "{{ task_instance.xcom_pull(task_ids='load_env_vars')['project_id'] }}", timezone = timezone('America/Sao_Paulo')): 
    google_chat_notification(context, webhook_url, timezone, VAR_MENSAGE='error')

# Definição da DAG
with DAG(
    dag_id              = "dag_kaggle_sample_sales",
    schedule_interval   = "0 6 * * *",  # Executa todos os dias às 6 da manhã
    start_date          = datetime(2025, 7, 1),
    catchup             = False,
    tags                = ["CloudFunction","BigQuery", "KaggleSampleSales"],
    on_failure_callback = lib_google_chat_notification_error,  # Aleta de falhas
) as dag:
    
    # 1.Carrega as variáveis no PythonOperator
    load_env_vars = PythonOperator(
        task_id         = "load_env_vars",
        python_callable = get_airflow_env_vars,
        provide_context = True
    )

    # 2.Task para invocar a Cloud Function
    trigger_cloud_function = CloudFunctionInvokeFunctionOperator(
        task_id         = "fnc_kaggle_sample_sales",
        project_id      = "{{ task_instance.xcom_pull(task_ids='load_env_vars')['project_id'] }}",
        location        = "{{ task_instance.xcom_pull(task_ids='load_env_vars')['region'] }}",
        function_id     = "{{ task_instance.xcom_pull(task_ids='load_env_vars')['function_id'] }}",
        gcp_conn_id     = "{{ task_instance.xcom_pull(task_ids='load_env_vars')['gcp_conn_id'] }}",
        input_data      = "{{ task_instance.xcom_pull(task_ids='load_env_vars')['input_data'] }}",
    )

    # Definição do fluxo
    load_env_vars >> trigger_cloud_function