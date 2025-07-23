from airflow                                            import DAG
from airflow.models                                     import Variable
from airflow.operators.python                           import PythonOperator

from datetime                                           import datetime
from pytz                                               import timezone

## Bibliotecas desenvolvidas pelo time no diretorio modules ##
from modules.google_chat_notification                   import notification_hook
from modules.invoke_cloud_function                      import post_requests

## FUNCOES ##
def get_airflow_env_vars(**context):
    """
    Função centralizada para importação das variáveis de ambiente do Airflow.
    Retorna um dicionário com todas as variáveis necessárias e faz push para o XCom.
    """
    environment_variables             = Variable.get('environment_variables', deserialize_json=True)
    fnc_get_kaggle_load_gcs_variables = Variable.get('fnc_get_kaggle_load_gcs_variables', deserialize_json=True)

    env_vars = {
        "project_id":   environment_variables['project_id'],
        "region":       environment_variables['region'],
        "webhook_url":  environment_variables['webhook_url'],
        "function_id":  fnc_get_kaggle_load_gcs_variables['function_id'],
        "input_data":   fnc_get_kaggle_load_gcs_variables['input_data']
    }

    context['ti'].xcom_push(key="env_vars", value=env_vars)
    return env_vars

def lib_google_chat_notification_error(context):
    """
    Callback de falha para enviar notificação ao Google Chat.
    """
    ti          = context['ti']
    webhook_url = ti.xcom_pull(task_ids='load_env_vars')['webhook_url']

    notification_hook(context, webhook_url, timezone('America/Sao_Paulo'), VAR_MENSAGE='error')

def post_fnc_get_kaggle_load_gcs(**context):
    """
    Invoca a Cloud Function Gen2 com os parâmetros necessários.
    """
    ti           = context["ti"]
    env_vars     = ti.xcom_pull(task_ids="load_env_vars")
    response = post_requests(env_vars['region'],env_vars['project_id'], env_vars['function_id'], env_vars['input_data'])
    
    return response

## DEFINIÇÃO DOS PARAMETROS DA DAG ##
with DAG(
    dag_id              = "dag_kaggle_sample_sales",
    schedule_interval   = "0 6 * * *",                                                   # Executa todos os dias às 6 da manhã
    start_date          = datetime(2025, 7, 1),
    catchup             = False,
    tags                = ["CloudFunction", "BigQuery", "KaggleSampleSales"],
    default_args        = {
                            'owner':               'Airflow - Data Engineering',
                            'start_date':          datetime(2025, 7, 1),
                            'on_failure_callback': lib_google_chat_notification_error,  # Notificação em caso de erro
                            'retries':              None,                               # Não reexecuta em caso de falha
                          }
) as dag:

    # 1. Carrega as variáveis e envia para o XCom
    load_env_vars = PythonOperator(
        task_id         = "load_env_vars",
        python_callable = get_airflow_env_vars,
        provide_context = True
    )

    # 2. Task para invocar a Cloud Function
    fnc_get_kaggle_load_gcs = PythonOperator(
        task_id         = "fnc_get_kaggle_load_gcs",
        python_callable = post_fnc_get_kaggle_load_gcs,
        provide_context = True
    )

    # Fluxo de Execução
    load_env_vars >> fnc_get_kaggle_load_gcs