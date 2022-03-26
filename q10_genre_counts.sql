with titles_genres(title_id, genres, rest) as (
    select titles.title_id, '', titles.genres || ','
    from titles
    where not titles.genres = '\N' and titles.genres not null
    union all
    select
    title_id,
    substr(rest, 0, instr(rest, ',')),
    substr(rest, instr(rest, ',') + 1)
    from titles_genres
    where not rest = ''
)
select genres, count(title_id) as genre_counts
from titles_genres
where not genres = ''
group by genres
order by genre_counts desc;
