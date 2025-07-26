from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator

def invoke_gbq_proc(task_id, query, region):
    """
    Invoca a procedure do BigQuery usando o BigQueryInsertJobOperator.
    """
    return BigQueryInsertJobOperator(
       task_id      = task_id,
       configuration= {
                        "query": {
                            "query": query,
                            "useLegacySql": False,
                        }
                    },
       location     = region
    )