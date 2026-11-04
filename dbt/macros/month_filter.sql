{#-
    Restrict a model to the month passed as --vars '{year_month: "YYYY-MM"}'.
    With no var set, nothing is filtered and every month is (re)built.
-#}
{% macro month_filter(column='year_month') -%}
    {%- if var('year_month', none) -%}
        where {{ column }} = '{{ var("year_month") }}'
    {%- endif -%}
{%- endmacro %}
