FROM astrocrpublic.azurecr.io/runtime:3.3-8

# apache-airflow-providers-microsoft-fabric (<= 0.1.1) registers an Airflow 2-only
# UI plugin (fabric_status_plugin) that imports airflow.www, which was removed in
# Airflow 3 -> "No module named 'airflow.www'" plugin import error. Unregister that
# entry point; the provider's operators/hooks/triggers are unaffected.
# Remove this block once the provider ships an Airflow 3 compatible plugin.
USER root
RUN sed -i '/^\[airflow\.plugins\]/,/^\[/{/^\[airflow\.plugins\]/d;/^\[/!d}' \
        /usr/local/lib/python3*/site-packages/apache_airflow_providers_microsoft_fabric-*.dist-info/entry_points.txt

# The same provider requires conn_type == "microsoft-fabric" exactly, but Airflow 3
# normalizes "-" to "_" for every connection loaded from the Astro Environment
# Manager, AIRFLOW_CONN_* env vars or a secrets backend, so the hook rejects
# fabric_default as "microsoft_fabric". Accept both spellings. The check runs in
# the worker and the triggerer (deferrable), hence an image-level patch.
# The grep fails the build if a provider upgrade changes this line.
RUN f=$(echo /usr/local/lib/python3*/site-packages/airflow/providers/microsoft/fabric/hooks/connection/rest_connection.py) \
    && sed -i 's/if conn\.conn_type != self\.conn_type:/if (conn.conn_type or "").replace("_", "-") != self.conn_type:/' "$f" \
    && grep -q 'conn.conn_type or "").replace("_", "-") != self.conn_type' "$f"
USER astro
