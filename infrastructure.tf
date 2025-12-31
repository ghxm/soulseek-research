# Simple Terraform for Soulseek Research
# Creates: 1 database server + N client servers with Docker pre-installed

terraform {
  required_providers {
    hcloud = {
      source = "hetznercloud/hcloud"
    }
  }
}

# Configure the Hetzner Cloud Provider
provider "hcloud" {
  token = var.hcloud_token
}

# Variables
variable "hcloud_token" {
  description = "Hetzner Cloud API Token"
  type        = string
  sensitive   = true
}

variable "db_password" {
  description = "Database password"
  type        = string
  sensitive   = true
}

variable "soulseek_credentials" {
  description = "Soulseek usernames and passwords for remote clients"
  type = map(object({
    username = string
    password = string
  }))
}

variable "germany_credentials" {
  description = "Germany client credentials (runs on database server)"
  type = object({
    username = string
    password = string
  })
}

# SSH Key - use existing key by name
data "hcloud_ssh_key" "default" {
  name = "soulseek-research"
}

# Database Server
resource "hcloud_server" "database" {
  name        = "soulseek-db"
  image       = "ubuntu-22.04"
  server_type = "cx33"
  location    = "nbg1"
  ssh_keys    = [data.hcloud_ssh_key.default.id]

  user_data = templatefile("${path.module}/setup-database.sh", {
    db_password = var.db_password
    germany_username = var.germany_credentials.username
    germany_password = var.germany_credentials.password
  })

  labels = {
    type = "database"
  }
}

# Client Servers with specific locations
resource "hcloud_server" "client" {
  for_each = var.soulseek_credentials

  name        = "soulseek-client-${each.key}"
  image       = "ubuntu-22.04" 
  server_type = each.key == "singapore" ? "cpx11" : "cpx11"
  location    = each.key == "us-west" ? "ash" : (each.key == "singapore" ? "sin" : "fsn1")
  ssh_keys    = [data.hcloud_ssh_key.default.id]

  user_data = templatefile("${path.module}/setup-client.sh", {
    db_host           = hcloud_server.database.ipv4_address
    db_password       = var.db_password
    soulseek_username = each.value.username
    soulseek_password = each.value.password
    client_id         = each.key
  })

  depends_on = [hcloud_server.database]

  labels = {
    type   = "client"
    region = each.key
  }
}

# Outputs
output "database_ip" {
  value = hcloud_server.database.ipv4_address
}

output "client_ips" {
  value = {
    for k, v in hcloud_server.client : k => v.ipv4_address
  }
}