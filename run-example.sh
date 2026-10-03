#!/bin/bash
# Usage: ./run-example.sh examples/01_faker_basics.sql
set -euo pipefail
case "${1:-}" in
  examples/*.sql) ;;
  *) echo "Usage: $0 examples/<file>.sql" >&2; exit 1 ;;
esac
docker compose exec -T sql-client /opt/sql-client/sql-client.sh -f "/opt/sql-client/${1}"
