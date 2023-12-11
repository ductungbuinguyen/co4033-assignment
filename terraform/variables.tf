locals {
  data_lake_bucket = "divvy_data_lake"
}

variable "project" {
  description = "co4033-assignment"
}

variable "region" {
  default = "asia-southeast1"
  type = string
}

variable "storage_class" {
  default = "STANDARD"
}

variable "BQ_DATASET" {
  type = string
  default = "divvy_data_raw"
}

variable "DBT_DATASET" {
  type = string
  default = "divvy_data_dbt"
}
