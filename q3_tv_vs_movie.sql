select type, count(*) as num_titles from titles
    group by type
    order by num_titles asc;