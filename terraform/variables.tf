variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "operator_ip" {
  description = "Operator's current public IPv4 address (bare IP, no CIDR suffix — the /32 is appended where consumed). No default: re-supply on each apply if it changes."
  type        = string
}

variable "project_name" {
  description = "Prefix used for resource names and tags across every Terraform-managed resource."
  type        = string
  default     = "airflow-dbt-pipeline"
}
