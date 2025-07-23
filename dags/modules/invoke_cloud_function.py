import requests
## Bibliotecas do para autenticação e manipulação de tokens ##
from google.auth.transport.requests  import Request
from google.oauth2                   import id_token

def get_identity_token(audience_url):
    """
    Obtém o Identity Token para autenticar na Cloud Function Gen2
    """
    return id_token.fetch_id_token(Request(), audience_url)

def post_requests (region, project_id, function_id, input_data):
    """
    Invoca a Cloud Function Gen2 com os parâmetros necessários.
    """

    audience_url = f"https://{region}-{project_id}.cloudfunctions.net/{function_id}"
    token        = get_identity_token(audience_url)
    
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