# person queries

load_person_by_date = f'''SELECT id, updated_at
                    FROM content.person
                    WHERE updated_at > '%s'
                    ORDER BY updated_at'''

load_person_from_film_work_id = f'''SELECT fw.id, fw.updated_at
                    FROM content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    WHERE pfw.person_id IN (%s)
                    ORDER BY fw.updated_at'''


# film_work queries

load_film_work = f'''SELECT DISTINCT fw.id,
                fw.title,
                fw.description,
                fw.rating,
                fw.type,
                fw.updated_at,
                ARRAY_AGG(DISTINCT jsonb_build_object('name', g.name, 'id', g.id)) AS genre,
                ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
                FILTER (WHERE pfw.role = 'director')                               AS director,
                ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
                FILTER (WHERE pfw.role = 'actor')                                  AS actors,
                ARRAY_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name))
                FILTER (WHERE pfw.role = 'writer')                                 AS writers,
                ARRAY_AGG(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'actor')  AS actors_names,
                ARRAY_AGG(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'writer') AS writers_names
FROM content.film_work as fw
         LEFT JOIN content.person_film_work as pfw ON pfw.film_work_id = fw.id
         LEFT JOIN content.person as p ON p.id = pfw.person_id
         LEFT JOIN content.genre_film_work as gfw ON gfw.film_work_id = fw.id
         LEFT JOIN content.genre as g ON g.id = gfw.genre_id'''

load_film_work_by_date = f'''{load_film_work} WHERE fw.updated_at > '%s' GROUP BY fw.id;'''

load_film_work_by_id = f'''{load_film_work} WHERE fw.id IN (%s) GROUP BY fw.id;'''


# genre queries

load_genre_by_date = f'''SELECT id, updated_at
                    FROM content.genre
                    WHERE updated_at > '%s'
                    ORDER BY updated_at'''

load_genre_from_film_work_id = f'''SELECT fw.id, fw.updated_at
                    FROM content.film_work fw
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                    WHERE gfw.genre_id IN (%s)
                    ORDER BY fw.updated_at'''
