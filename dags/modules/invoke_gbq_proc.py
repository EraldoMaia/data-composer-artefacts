from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator

def invoke_gbq_proc(proc_project, origin_project, destination_project, table_name, dataset_name, region):
    """
    Invokes a BigQuery stored procedure to load data into the trusted table.
    """
    return BigQueryInsertJobOperator(
        task_id=f'prc_load_{table_name}',
        configuration={
            "query": {
                "query": f"""
                    CALL `{proc_project}.procs.prc_load_{table_name}`(
                        '{origin_project}',
                        '{destination_project}',
                        '{table_name}',
                        '{dataset_name}'
                    );
                """,
                "useLegacySql": False
            }
        },
        location=f"{region}",
    )