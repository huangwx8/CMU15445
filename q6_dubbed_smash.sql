select titles.primary_title, count(*) as num_akas
    from titles, akas
    where titles.title_id = akas.title_id
    group by titles.title_id, titles.primary_title
    order by num_akas desc
    limit 10;