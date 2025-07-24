# data-composer-artefacts

Para poder usar a intragracao entre os projetos é necessario fornecer os acessos ao GBQ a conta de servico dentro do projeto especifico.

1. Camada raw:
   ```
   gcloud config set project raw

   gcloud projects add-iam-policy-binding raw \
     --member="serviceAccount:composer-sa@data-ops-466417.iam.gserviceaccount.com" \
     --role="roles/bigquery.jobUser"

   gcloud projects add-iam-policy-binding raw \
     --member="serviceAccount:composer-sa@data-ops-466417.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataEditor"
   ```

Para bronze e silver repete o mesmo processo mudando o projeto.
