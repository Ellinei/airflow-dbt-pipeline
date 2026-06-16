{% macro grant_select(role) %}
  GRANT USAGE ON SCHEMA {{ this.schema }} TO {{ role }};
  GRANT SELECT ON {{ this }} TO {{ role }};
{% endmacro %}
