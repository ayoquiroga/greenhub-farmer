# =========================================================
# Variables configurables del proyecto
# =========================================================

variable "project_id" {
  description = "ID del proyecto GCP"
  type        = string
  default     = "kestra-sandbox-2026"
}

variable "region" {
  description = "Región GCP para todos los recursos"
  type        = string
  default     = "us-central1"
}

variable "credentials" {
  description = "Ruta al archivo JSON de la cuenta de servicio GCP"
  type        = string
  default     = "../kestra-sandbox-2026-86d1525dea28.json"
}

variable "bucket_name" {
  description = "Nombre del bucket de Cloud Storage (Data Lake). Debe ser globalmente único."
  type        = string
  default     = "greenhub-raw-kestra-2026"
}

variable "bq_dataset" {
  description = "ID del dataset de BigQuery (Data Warehouse)"
  type        = string
  default     = "greenhub"
}
