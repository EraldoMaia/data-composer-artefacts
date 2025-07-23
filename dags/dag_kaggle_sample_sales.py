import requests
from airflow                                            import DAG
from airflow.models                                     import Variable
from airflow.operators.python                           import PythonOperator

from datetime                                           import datetime
from pytz                                               import timezone

## Bibliotecas do para autenticação e manipulação de tokens ##
from google.auth.transport.requests                     import Request
from google.oauth2                                      import id_token

## Bibliotecas desenvolvidas pelo time no diretorio modules ##
from modules.google_chat_notification                   import notification_hook


## FUNCOES ##
def get_identity_token(audience_url):
    """
    Obtém o Identity Token para autenticar na Cloud Function Gen2
    """
    return id_token.fetch_id_token(Request(), audience_url)


def generate_identity_token_callable(**kwargs):
    """
    Gera o Identity Token e salva no XCom.
    """
    ti           = kwargs["ti"]
    env_vars     = ti.xcom_pull(task_ids="load_env_vars")

    audience_url = f"https://{env_vars['region']}-{env_vars['project_id']}.cloudfunctions.net/{env_vars['function_id']}"
    token        = get_identity_token(audience_url)

    ti.xcom_push(key="identity_token", value=token)
    ti.xcom_push(key="audience_url", value=audience_url)

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

def post_fnc_kaggle_sample_sales(**kwargs):
    """
    Invoca a Cloud Function Gen2 com os parâmetros necessários.
    """

    ti           = kwargs["ti"]
    audience_url = ti.xcom_pull(task_ids="generate_identity_token", key="audience_url")
    token        = ti.xcom_pull(task_ids="generate_identity_token", key="identity_token")
    input_data   = ti.xcom_pull(task_ids="load_env_vars")["input_data"]

    response = requests.post(
        audience_url,
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json    = input_data,
        timeout = 120
    )

    response.raise_for_status()
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
    return response.text

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

    # 2. Gera o Bearer Token para autenticação
    generate_identity_token = PythonOperator(
        task_id         = "generate_identity_token",
        python_callable = generate_identity_token_callable,
        provide_context = True,
    )

    # 3. Task para invocar a Cloud Function
    fnc_kaggle_sample_sales = PythonOperator(
        task_id         = "fnc_kaggle_sample_sales",
        python_callable = post_fnc_kaggle_sample_sales,
        provide_context = True
    )

    # Fluxo de Execução
    load_env_vars >> generate_identity_token >> fnc_kaggle_sample_sales