select ((premiered / 10) || '0s') as decade, count(*) as num_titles
    from titles
    where premiered not null
    group by decade
    order by num_titles desc;