{#- Overrides of dbt-doris 1.0.0 macros (project macros take precedence over the adapter's). -#}

{#- dbt-doris renders the model description unescaped into COMMENT '...',
    so a description containing `'` (e.g. "customer's") breaks the CREATE TABLE. -#}
{% macro doris__table_comment() -%}
  {% set description = model.get('description', "") %}
  COMMENT '{{ description | replace("\\", "\\\\") | replace("'", "\\'") }}'
{%- endmacro %}

{#- dbt-doris wraps the model in `select <columns from schema.yml> from (...)` whenever columns are
    documented: undocumented columns are silently dropped and doc-only columns fail the CTAS.
    Like dbt-core, only project/cast to the declared columns when the contract is enforced. -#}
{% macro doris__table_colume_type(sql) -%}
    {% if config.get('contract', {}).enforced %}
        select {{ get_table_columns_and_constraints() }} from (
            {{ sql }}
        ) `_table_colume_type_name`
    {% else %}
        {{ sql }}
    {%- endif -%}
{%- endmacro %}
