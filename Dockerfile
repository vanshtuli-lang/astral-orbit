FROM astrocrpublic.azurecr.io/runtime:3.3-8

# apache-airflow-providers-microsoft-fabric (<= 0.1.1) registers an Airflow 2-only
# UI plugin (fabric_status_plugin) that imports airflow.www, which was removed in
# Airflow 3 -> "No module named 'airflow.www'" plugin import error. Unregister that
# entry point; the provider's operators/hooks/triggers are unaffected.
# Remove this block once the provider ships an Airflow 3 compatible plugin.
USER root
RUN sed -i '/^\[airflow\.plugins\]/,/^\[/{/^\[airflow\.plugins\]/d;/^\[/!d}' \
        /usr/local/lib/python3*/site-packages/apache_airflow_providers_microsoft_fabric-*.dist-info/entry_points.txt
USER astro
