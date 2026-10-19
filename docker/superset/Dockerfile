# Image lean không kèm driver Postgres.
FROM apache/superset:6.1.0

USER root
RUN . /app/.venv/bin/activate && uv pip install --no-cache psycopg2-binary
COPY superset_config.py /app/superset_config.py
COPY --chmod=755 entrypoint.sh /app/btl-entrypoint.sh
USER superset

CMD ["/app/btl-entrypoint.sh"]
