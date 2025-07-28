from airflow                                            import DAG
from airflow.models                                     import Variable
from airflow.operators.python                           import PythonOperator
from airflow.operators.dummy                            import DummyOperator
from airflow.models.baseoperator                        import chain


from datetime                                           import datetime
from pytz                                               import timezone

## Bibliotecas desenvolvidas pelo time no diretorio modules ##
from modules.google_chat_notification                   import notification_hook
from modules.invoke_cloud_function                      import post_requests
from modules.invoke_gbq_proc                            import invoke_gbq_proc
from modules.check_carga                                import check_carga_media_log

## FUNCOES ##
def get_airflow_env_vars(**context):
    """
    Função centralizada para importação das variáveis de ambiente do Airflow.
    Retorna um dicionário com todas as variáveis necessárias e faz push para o XCom.
    """
    environment_variables                               = Variable.get('environment_variables',                             deserialize_json=True)
    fnc_get_kaggle_load_gcs_variables                   = Variable.get('fnc_get_kaggle_load_gcs_variables',                 deserialize_json=True)
    fnc_get_gcs_load_gbq_variables                      = Variable.get('fnc_get_gcs_load_gbq_variables',                    deserialize_json=True)
    prc_load_trusted_tb_sample_sales_variables          = Variable.get('prc_load_trusted_tb_sample_sales_variables',        deserialize_json=True)
    prc_load_refined_tb_top10_line_products_variables   = Variable.get('prc_load_refined_tb_top10_line_products_variables', deserialize_json=True)


    env_vars = {
        "project_id":                   environment_variables['project_id'],
        "region":                       environment_variables['region'],
        "webhook_error_url":            environment_variables['webhook_error_url'],
        "webhook_success_url":          environment_variables['webhook_success_url'],
        "var_prj_raw":                  environment_variables['var_prj_raw'],
        "var_prj_trusted":              environment_variables['var_prj_trusted'],
        "var_prj_refined":              environment_variables['var_prj_refined'],
        # Variáveis específicas para as Cloud Functions
        "function_id_kaggle_gcs":       fnc_get_kaggle_load_gcs_variables['function_id'],
        "input_data_kaggle_gcs":        fnc_get_kaggle_load_gcs_variables['input_data'],
        "function_id_gcs_gbq":          fnc_get_gcs_load_gbq_variables['function_id'],
        "input_data_gcs_gbq":           fnc_get_gcs_load_gbq_variables['input_data'],
        # Variáveis especificas para procidures do BigQuery
        "var_tb_sample_sales":          prc_load_trusted_tb_sample_sales_variables['var_tabela'],
        "var_tb_top10_line_products":   prc_load_refined_tb_top10_line_products_variables['var_tabela'],
        "var_dataset_kaggle":           prc_load_trusted_tb_sample_sales_variables['var_dataset'],
        # Variaveis específicas para a verificação de carga
        "dias_media":          1,
        "tolerancia_inferior": 0.8,
        "tolerancia_superior": 3,

    }

    context['ti'].xcom_push(key="env_vars", value=env_vars)
    return env_vars

def lib_google_chat_notification_error(context):
    """
    Callback de falha para enviar notificação ao Google Chat.
    """
    ti          = context['ti']
    webhook_url = ti.xcom_pull(task_ids='load_env_vars')['webhook_error_url']

    return notification_hook(
        context, 
        webhook_url, 
        timezone('America/Sao_Paulo'), 
        VAR_MENSAGE='error'
    )

def lib_google_chat_notification_success(context):
    """
    Callback de falha para enviar notificação ao Google Chat.
    """
    ti          = context['ti']
    webhook_url = ti.xcom_pull(task_ids='load_env_vars')['webhook_success_url']

    return notification_hook(
        context, 
        webhook_url, 
        timezone('America/Sao_Paulo'), 
        VAR_MENSAGE='success'
    )

def invoke_fnc_get_kaggle_load_gcs(**context):
    """
    Invoca a Cloud Function Gen2 que carrega os dados do Kaggle para o Google Cloud Storage (GCS).
    """
    ti           = context["ti"]
    env_vars     = ti.xcom_pull(task_ids="load_env_vars")

    return post_requests(
        region      = env_vars['region'],
        project_id  = env_vars['project_id'], 
        function_id = env_vars['function_id_kaggle_gcs'], 
        input_data  = env_vars['input_data_kaggle_gcs']
    )

def invoke_fnc_get_gcs_load_gbq(**context):
    """
    Invoca a Cloud Function Gen2 que carrega os dados do GCS para o BigQuery.
    """
    ti           = context["ti"]
    env_vars     = ti.xcom_pull(task_ids="load_env_vars")

    return post_requests(
        region      = env_vars['region'],
        project_id  = env_vars['project_id'], 
        function_id = env_vars['function_id_gcs_gbq'], 
        input_data  = env_vars['input_data_gcs_gbq']
    )

## DEFINIÇÃO DOS PARAMETROS DA DAG ##
with DAG(
    dag_id              = "dag_kaggle_sample_sales",
    schedule_interval   = "0 6 * * *",                                                   # Executa todos os dias às 6 da manhã
    start_date          = datetime(2025, 7, 1),
    catchup             = False,
    tags                = ["CloudFunction", "CloudStorage", "BigQuery", "KaggleSampleSales"],
    default_args        = {
                            'owner':               'Airflow - Data Engineering',
                            'start_date':          datetime(2025, 7, 1),
                            'on_failure_callback': lib_google_chat_notification_error,   # Notificação em caso de erro
                            'on_success_callback': lib_google_chat_notification_success, # Notificação em caso de sucesso
                            'retries':              None,                                # Não reexecuta em caso de falha
                          }
) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    # 1. Carrega as variáveis e envia para o XCom
    load_env_vars = PythonOperator(
        task_id         = "load_env_vars",
        python_callable = get_airflow_env_vars,
        provide_context = True
    )

    # 2. Task para invocar a Cloud Function, que carrega os dados do Kaggle para o GCS
    fnc_get_kaggle_load_gcs = PythonOperator(
        task_id         = "fnc_get_kaggle_load_gcs",
        python_callable = invoke_fnc_get_kaggle_load_gcs,
        provide_context = True
    )

    # 3. Task para invocar a Cloud Function, que carrega os dados do GCS para o BigQuery
    fnc_get_gcs_load_gbq = PythonOperator(
        task_id         = "fnc_get_gcs_load_gbq",
        python_callable = invoke_fnc_get_gcs_load_gbq,
        provide_context = True
    )

    # 4. Task para carregar os dados do BigQuery para a camada trusted
    prc_load_tb_sample_sales = invoke_gbq_proc(
        task_id = "prc_load_tb_sample_sales",
        query   = """
                    CALL `{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['var_prj_trusted'] }}.procs.prc_load_tb_sample_sales` (
                        '{{ ti.xcom_pull(task_ids="load_env_vars", key="env_vars")["var_prj_raw"] }}',
                        '{{ ti.xcom_pull(task_ids="load_env_vars", key="env_vars")["var_prj_trusted"] }}',
                        '{{ ti.xcom_pull(task_ids="load_env_vars", key="env_vars")["var_tb_sample_sales"] }}',
                        '{{ ti.xcom_pull(task_ids="load_env_vars", key="env_vars")["var_dataset_kaggle"] }}'
                    );
                    """
    )

    # 5. Task para carregar os dados do BigQuery para a camada refined
    prc_load_tb_top10_line_products = invoke_gbq_proc(
        task_id = "prc_load_tb_top10_line_products",
        query   = """
                    CALL `{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['var_prj_refined'] }}.procs.prc_load_tb_top10_line_products` (
                        '{{ ti.xcom_pull(task_ids="load_env_vars", key="env_vars")["var_prj_trusted"] }}',
                        '{{ ti.xcom_pull(task_ids="load_env_vars", key="env_vars")["var_prj_refined"] }}',
                        '{{ ti.xcom_pull(task_ids="load_env_vars", key="env_vars")["var_tb_top10_line_products"] }}',
                        '{{ ti.xcom_pull(task_ids="load_env_vars", key="env_vars")["var_dataset_kaggle"] }}'
                    );
                    """    
    )
    
    # 6. Task para verificar se a carga do dia está com a quantidade de linhas dentro do intervalo aceitável (camada raw)
    check_carga_raw_sample_sales = check_carga_media_log(
            task_id             = "check_carga_raw_sample_sales",
            project_id          = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['var_prj_raw'] }}",
            dataset_id          = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['var_dataset_kaggle'] }}",
            table_name          = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['input_data_gcs_gbq']['file_prefix'] }}",
            dias_media          = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['dias_media'] }}",
            tolerancia_inferior = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['tolerancia_inferior'] }}", 
            tolerancia_superior = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['tolerancia_superior'] }}"   
    )

    # 7. Task para verificar se a carga do dia está com a quantidade de linhas dentro do intervalo aceitável (camada trusted)
    check_carga_trusted_tb_sample_sales = check_carga_media_log(
            task_id             = "check_carga_trusted_tb_sample_sales",
            project_id          = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['var_prj_trusted'] }}",
            dataset_id          = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['var_dataset_kaggle'] }}",
            table_name          = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['var_tb_sample_sales'] }}",
            dias_media          = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['dias_media'] }}",
            tolerancia_inferior = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['tolerancia_inferior'] }}", 
            tolerancia_superior = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['tolerancia_superior'] }}"   
    )

    # 8. Task para verificar se a carga do dia está com a quantidade de linhas dentro do intervalo aceitável (camada refined)
    check_carga_refined_tb_top10_line_products = check_carga_media_log(
            task_id             = "check_carga_refined_tb_top10_line_products",
            project_id          = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['var_prj_refined'] }}",
            dataset_id          = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['var_dataset_kaggle'] }}",
            table_name          = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['var_tb_top10_line_products'] }}",
            dias_media          = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['dias_media'] }}",
            tolerancia_inferior = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['tolerancia_inferior'] }}", 
            tolerancia_superior = "{{ ti.xcom_pull(task_ids='load_env_vars', key='env_vars')['tolerancia_superior'] }}"   
    )

    end = DummyOperator(
        task_id = "end"
    )   

    join = DummyOperator(
        task_id      = 'join',
        params       = {'description': 'Unifica fluxos do pipeline'},
        trigger_rule = 'all_done'
    )

    # Lista de tarefas para o fluxo de execucao da camada raw, trusted e refined
    raw_pipe     = [
                    load_env_vars,
                    fnc_get_kaggle_load_gcs,
                    fnc_get_gcs_load_gbq,
                    check_carga_raw_sample_sales
                    ]
        
    trusted_pipe = [
                    prc_load_tb_sample_sales, 
                    check_carga_trusted_tb_sample_sales
                    ]
    
    refined_pipe = [
                    prc_load_tb_top10_line_products,
                    check_carga_refined_tb_top10_line_products
                    ]

    # Fluxo de execucao da DAG
    # Unifica as tarefas de cada camada e conecta ao final
    chain(start, *raw_pipe, *trusted_pipe, *refined_pipe, end)