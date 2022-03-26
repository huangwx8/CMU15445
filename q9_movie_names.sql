with mark(person_id) as (
    select person_id
    from people
    where name = "Mark Hamill" and born == 1951
),
george(person_id) as (
    select person_id
    from people
    where name = "George Lucas" and born == 1944
),
titles_have_mark(title_id) as (
    select distinct crew.title_id
    from crew
    where crew.person_id in mark
),
titles_have_george(title_id) as (
    select distinct crew.title_id
    from crew
    where crew.person_id in george
)
select titles.primary_title
from titles
where titles.type = 'movie' and titles.title_id in (select * from titles_have_mark intersect select * from titles_have_george);