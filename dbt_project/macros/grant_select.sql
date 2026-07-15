{% macro grant_select(role) %}
  -- Advisory lock keyed by schema name: serializes the schema-level GRANT
  -- across concurrent model builds targeting the same schema (dbt threads,
  -- or separate Cosmos-per-model dbt invocations under Airflow), which
  -- otherwise race on pg_namespace's ACL and raise "tuple concurrently
  -- updated". Transaction-scoped, so it releases automatically per model.
  select pg_advisory_xact_lock(hashtext('{{ this.schema }}'));
  GRANT USAGE ON SCHEMA {{ this.schema }} TO {{ role }};
  GRANT SELECT ON {{ this }} TO {{ role }};
{% endmacro %}
