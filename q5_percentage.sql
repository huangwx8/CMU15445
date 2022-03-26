select ((premiered / 10) || '0s') as decade, round(count(*) * 100.0 / (select count(*) from titles), 4) as percetage
    from titles
    where premiered not null
    group by decade
    order by percetage desc;
