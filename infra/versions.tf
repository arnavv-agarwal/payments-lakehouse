# Terraform and provider versions. The exact provider build is recorded in
# .terraform.lock.hcl (committed), so every machine installs the same one.
terraform {
  required_version = ">= 1.14"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.70"
    }
  }

  # State is kept locally (infra/terraform.tfstate, gitignored). This project
  # lives for a few days and has one operator; a team would keep state in S3
  # with locking so two people can't apply at the same time.
}

provider "aws" {
  region = var.region

  # Every resource gets these tags, so the bill and the console can be
  # filtered to this project.
  default_tags {
    tags = {
      Project   = "payments-lakehouse"
      ManagedBy = "terraform"
    }
  }
}
