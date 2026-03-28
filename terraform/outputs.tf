# =========================================================
# Outputs — valores útiles tras el apply
# =========================================================

output "bucket_url" {
  description = "URL del bucket de Cloud Storage (Data Lake)"
  value       = "gs://${google_storage_bucket.raw.name}"
}

output "bucket_name" {
  description = "Nombre del bucket (para usar en el DAG de Airflow)"
  value       = google_storage_bucket.raw.name
}

output "bq_dataset" {
  description = "ID del dataset de BigQuery (Data Warehouse)"
  value       = google_bigquery_dataset.greenhub.dataset_id
}

output "bq_dataset_location" {
  description = "Región donde está el dataset de BigQuery"
  value       = google_bigquery_dataset.greenhub.location
}
