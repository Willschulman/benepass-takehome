#!/usr/bin/env bash
set -euo pipefail

ssh benepass "cd benepass-takehome && git pull && docker compose -f docker-compose.prod.yml up -d --build"