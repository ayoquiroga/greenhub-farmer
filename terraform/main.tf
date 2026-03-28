# =========================================================
# GreenHub Farmer — Infraestructura GCP con Terraform
#
# Proyecto: kestra-sandbox-2026
# Recursos: Cloud Storage bucket (Data Lake) + BigQuery dataset (Data Warehouse)
# =========================================================

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.5"
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project_id
  region      = var.region
}

# ── Data Lake: Cloud Storage bucket ──────────────────────
resource "google_storage_bucket" "raw" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true        # permite destruir aunque tenga objetos (útil en dev)
  storage_class = "STANDARD"

  # Borrar automáticamente objetos después de 90 días (ahorro de costos)
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90
    }
  }

  # Evitar acceso público accidental
  uniform_bucket_level_access = true
}

# ── Data Warehouse: BigQuery dataset ─────────────────────
resource "google_bigquery_dataset" "greenhub" {
  dataset_id                 = var.bq_dataset
  location                   = var.region
  delete_contents_on_destroy = true  # permite destruir aunque tenga tablas
}
