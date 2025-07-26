import requests

# Funcao de notificaco de erro para o Google Chat
def notification_hook(context, VAR_WEBHOOK_URL, VAR_TIMEZONE, VAR_MENSAGE):
    # Converter a execution_date para o fuso horario de 'America/Sao_Paulo'
    execution_date = context['execution_date'].astimezone(VAR_TIMEZONE)
    # Formatar a execution_date para remover os milissegundos
    formatted_execution_date = execution_date.strftime('%Y-%m-%d %H:%M:%S')

    # Obter a URL do log da task
    log_url = context.get('task_instance').log_url  # URL para acessar o log completo no Airflow UI

    # Definir o ti­tulo e subti­tulo da mensagem com base no tipo de mensagem
    if VAR_MENSAGE == 'error':
        title = f"âŒ <b>[{formatted_execution_date}]</b> Falha na execucao da task: {context['task_instance'].task_id}!"
        subtitle = f"DAG: {context['dag'].dag_id}"
        
    elif VAR_MENSAGE == 'success':
        title = f"âœ… <b>[{formatted_execution_date}]</b> Sucesso na execucao da task: {context['task_instance'].task_id}!"
        subtitle = f"DAG: {context['dag'].dag_id}"

    # Construir a mensagem no formato de um card
    message = {
        "cards": [
            {
                "header": {
                    "title": title,
                    "subtitle": subtitle
                },
                "sections": [
                    {
                        "widgets": [
                            {
                                "textParagraph": {
                                    "text": f"A task <b>{context['task_instance'].task_id}</b> na DAG <b>{context['dag'].dag_id}</b> teve um resultado de <b>{VAR_MENSAGE}</b>."
                                }
                            },
                            {
                                "buttons": [
                                    {
                                        "textButton": {
                                            "text": "VIEW FULL LOG",
                                            "onClick": {
                                                "openLink": {
                                                    "url": log_url
                                                }
                                            }
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }

    # Enviar a mensagem para o Google Chat
    response = requests.post(VAR_WEBHOOK_URL, json=message)

    # Verificar se a mensagem foi enviada com sucesso
    if response.status_code != 200:
        raise Exception(f"Falha ao enviar a mensagem para o Google Chat: {response.status_code}, {response.text}")