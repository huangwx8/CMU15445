with 
    mark(person_id) as (
        select person_id
        from people
        where name = "Mark Hamill" and born == 1951
    ),
    titles_have_mark(title_id) as (
        select distinct crew.title_id
        from crew
        where crew.person_id in mark
    )
select count(distinct crew.person_id)
    from crew
    where crew.title_id in titles_have_mark and (crew.category = 'actor' or crew.category = 'actress');