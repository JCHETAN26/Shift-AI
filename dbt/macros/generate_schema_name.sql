{# Use the model's configured +schema as the literal schema name (STAGING, GOLD)
   instead of dbt's default <target>_<custom> concatenation. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
