# Superset with the Athena driver + our provisioning script baked in.
# Pinned to the 6.0.0 release (verified Oct 2026); extend-the-lean-image
# is the officially recommended way to add database drivers.
FROM apache/superset:6.0.0

USER root
# NB: `python`/`superset` run from /app/.venv — plain `pip` targets system
# site-packages, which is NOT on sys.path. The venv ships without pip
# (uv-built), so bootstrap it first, then install into the venv explicitly.
RUN /app/.venv/bin/python -m ensurepip \
    && /app/.venv/bin/python -m pip install --no-cache-dir "pyathena[pandas]"
COPY superset/superset_config.py /app/superset_config.py
COPY superset/bootstrap.sh /app/bootstrap.sh
COPY src/pipeline/superset /app/provision
RUN chmod +x /app/bootstrap.sh && chown -R superset:superset /app/provision

USER superset
# SUPERSET_CONFIG is a Python *module name* (imported, not a file path),
# resolved via PYTHONPATH.
ENV PYTHONPATH=/app SUPERSET_CONFIG=superset_config
ENTRYPOINT ["/app/bootstrap.sh"]
