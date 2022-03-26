with titles_by_type(type, max_runtime_minutes) as (
    select titles.type, max(titles.runtime_minutes) from titles group by type
)

select titles.type, titles.primary_title, titles.runtime_minutes
    from titles, titles_by_type
    where titles.type = titles_by_type.type and titles.runtime_minutes >= titles_by_type.max_runtime_minutes
    order by titles.type asc, primary_title asc;