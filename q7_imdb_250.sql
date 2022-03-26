with 
    ratings_wrapper(title_id, R, v, m) as (
        select ratings.title_id, ratings.rating, ratings.votes, 25000 from ratings
    ),
    avg_rating(C) as (
        select sum(v*R) / sum(v)
        from titles, ratings_wrapper
        where titles.type = 'movie' and titles.title_id = ratings_wrapper.title_id
    ),
    w_avg_rating(C) as (
        select sum(v*1.0/(v+m)*R) / (count(*)-sum(m*1.0/(v+m)))
        from titles, ratings_wrapper
        where titles.type = 'movie' and titles.title_id = ratings_wrapper.title_id
    )
select titles.primary_title, ((v*1.0/(v+m))*R + (m*1.0/(v+m))*C) as w_rating
    from titles, ratings_wrapper, avg_rating
    where titles.type = 'movie' and titles.title_id = ratings_wrapper.title_id
    order by w_rating desc
    limit 250;