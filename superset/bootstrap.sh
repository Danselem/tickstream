#!/usr/bin/env bash
# Superset container entrypoint: init metadata DB, ensure admin, start the
# server, then provision the Athena database + dashboard (idempotent, so
# restarts and rebuilds converge without duplicating anything).
set -euo pipefail

: "${SUPERSET_ADMIN_USER:?set SUPERSET_ADMIN_USER (see .env.example)}"
: "${SUPERSET_ADMIN_PASSWORD:?set SUPERSET_ADMIN_PASSWORD (see .env.example)}"
: "${SUPERSET_ADMIN_EMAIL:?set SUPERSET_ADMIN_EMAIL (see .env.example)}"
: "${SUPERSET_URL:=http://localhost:8088}"

superset db upgrade
superset init

if ! superset fab list-users 2>/dev/null | grep -q "username: *${SUPERSET_ADMIN_USER}"; then
  superset fab create-admin \
    --username "${SUPERSET_ADMIN_USER}" \
    --firstname tickstream --lastname pipeline \
    --email "${SUPERSET_ADMIN_EMAIL}" \
    --password "${SUPERSET_ADMIN_PASSWORD}"
fi

# Server in background so we can provision against its API, then wait on it.
/app/docker/entrypoints/run-server.sh &
SERVER_PID=$!

echo "waiting for superset at ${SUPERSET_URL} ..."
for _ in $(seq 1 60); do
  if curl -fsS "${SUPERSET_URL}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
curl -fsS "${SUPERSET_URL}/health" >/dev/null

# Provisioning is idempotent; a failure here must not take the UI down with it.
python /app/provision/provision.py || echo "WARNING: dashboard provisioning failed; UI still serving"

wait "${SERVER_PID}"
