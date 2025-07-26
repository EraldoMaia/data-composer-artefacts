from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

def check_carga_media_log(task_id, project_id, dataset_id, table_name,
                          dias_media, tolerancia_inferior, tolerancia_superior):
    """
    Valida se a carga do dia está dentro de um intervalo aceitável baseado na média dos últimos dias,
    usando a tabela de log de execução (tb_log_exec).
    """
    return BigQueryCheckOperator(
        task_id=task_id,
        sql=f"""
            WITH log_filtrado AS (
                SELECT *
                FROM `data-ops-466417.data_quality.tb_log_exec`
                WHERE 
                      projeto    = '{project_id}'
                  AND dataset    = '{dataset_id}'
                  AND table_name = '{table_name}'
                  AND DATE(dt_insercao_registro) <= CURRENT_DATE()
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY projeto, dataset, table_name, DATE(dt_insercao_registro)
                    ORDER BY dt_insercao_registro DESC
                ) = 1
            ),
            media_diaria AS (
                SELECT 
                    COALESCE(SAFE_DIVIDE(SUM(qtd_linhas), COUNT(*)), 1) AS media_registros
                FROM log_filtrado
                WHERE DATE(dt_insercao_registro) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {dias_media} DAY)
                                                     AND DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY)
                  AND qtd_linhas > 0
            ),
            carga_hoje AS (
                SELECT COALESCE(SUM(qtd_linhas), 0) AS qtd_hoje
                FROM log_filtrado
                WHERE DATE(dt_insercao_registro) = CURRENT_DATE()
            )
            SELECT 
                (carga_hoje.qtd_hoje >= media_diaria.media_registros * {tolerancia_inferior})
                AND
                (carga_hoje.qtd_hoje <= media_diaria.media_registros * {tolerancia_superior})
            FROM carga_hoje, media_diaria
        """,
        use_legacy_sql=False,
        location="southamerica-east1"
    )
