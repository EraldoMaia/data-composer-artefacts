from airflow                                            import DAG
from airflow.models                                     import Variable
from airflow.providers.http.operators.http              import SimpleHttpOperator
from airflow.operators.python                           import PythonOperator
from datetime                                           import datetime
from pytz                                               import timezone
from json                                               import dumps as json_dumps
## Bibliotecas desenvolvidas pelo time no diretorio modules ##
from modules.google_chat_notification                   import notification_hook


## FUNCOES ##
def get_airflow_env_vars(**context):
    """
    Função centralizada para importação das variáveis de ambiente do Airflow.
    Retorna um dicionário com todas as variáveis necessárias e faz push para o XCom.
    """
    environment_variables             = Variable.get('environment_variables', deserialize_json=True)
    fnc_kaggle_sample_sales_variables = Variable.get('fnc_kaggle_sample_sales_variables', deserialize_json=True)

    env_vars = {
        "project_id":   environment_variables['project_id'],
        "region":       environment_variables['region'],
        "webhook_url":  environment_variables['webhook_url'],
        "function_id":  fnc_kaggle_sample_sales_variables['function_id'],
        "input_data":   fnc_kaggle_sample_sales_variables['input_data']
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
    fnc_kaggle_sample_sales = SimpleHttpOperator(
        task_id         = "fnc_extract_api_correios",
        method          = "POST",
        http_conn_id    = "google_cloud_function_http_connection",  
        endpoint        = "/fnc-kaggle-sample-sales",
        data            = json_dumps({

        }),
        headers         = {"Content-Type": "application/json"},
    )

    # Definição do fluxo
    load_env_vars >> fnc_kaggle_sample_sales