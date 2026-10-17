{% test unknown_member_ratio(model, column_name, max_ratio=0.05) %}
{#- Fails when more than max_ratio of the rows fell back to the UNKNOWN (-1) member or have no key
    (the same 5 % tolerance as the Spark gold DQ gate). -#}
select unresolved, total
from (
    select
        count_if(coalesce({{ column_name }}, -1) = -1) as unresolved,
        count(*) as total
    from {{ model }}
)
where unresolved > {{ max_ratio }} * total
{% endtest %}
