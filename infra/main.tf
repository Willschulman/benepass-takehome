terraform {
  cloud {
    organization = "benepass-takehome"

    workspaces {
      name = "benepass-takehome"
    }
  }

  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.49"
    }
  }

  required_version = ">= 1.5"
}

provider "hcloud" {
  token = var.hcloud_token
}

resource "hcloud_ssh_key" "benepass" {
  name       = "benepass"
  public_key = var.ssh_public_key
}

resource "hcloud_firewall" "benepass" {
  name = "benepass"

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22"
    source_ips = ["0.0.0.0/0", "::/0"]
  }

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "80"
    source_ips = ["0.0.0.0/0", "::/0"]
  }

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "443"
    source_ips = ["0.0.0.0/0", "::/0"]
  }
}

resource "hcloud_server" "benepass" {
  name         = "benepass"
  server_type  = "cax11"
  image        = "ubuntu-24.04"
  location     = "nbg1"
  ssh_keys     = [hcloud_ssh_key.benepass.id]
  firewall_ids = [hcloud_firewall.benepass.id]
  user_data    = file("cloud-init.yml")
}

output "server_ip" {
  value = hcloud_server.benepass.ipv4_address
}