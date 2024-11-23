import requests
from neo4j import GraphDatabase
from keys import API_KEY, DB_AUTH # Абстрагируем авторизационные данные

# Функция для получения данных о фильмах
def get_movie_data(movies: list[str]) -> list:
    res = []
    headers = {"accept": "application/json",
                   "Authorization": f"Bearer {API_KEY}"}
    with requests.session() as sess:
        
        for id in movies:
            url = f"https://api.themoviedb.org/3/movie/{id}?language=en-US"
            response = sess.get(url,
                                headers = headers).json()
    
            res.append({'year': response['release_date'][:4],
                        'languages': list(item['english_name'] for item in response['spoken_languages']),
                        'imdbId': response['imdb_id'],
                        'runtime': response['runtime'],
                        'imdbRating': response['vote_average'],
                        'movieId': response['id'],
                        'countries': list(item['name'] for item in response['production_countries']),
                        'imdbVotes': response['vote_count'],
                        'title': response['title'],
                        'url': f'https://www.themoviedb.org/movie/{id}',
                        'revenue': response['revenue'],
                        'tmdbId': id,
                        'plot': response['overview'].replace('"', "'"),
                        'poster': response['poster_path'],
                        'released': response['release_date'],
                        'budget': response['budget']
                        })
    return res

# Функция для получения данных оБ актерах и режиссерах
def get_people_data(movies: list[str]) -> tuple[list]:
    actors = []
    directors = []
    headers = {"accept": "application/json",
                   "Authorization": f"Bearer {API_KEY}"}
    with requests.session() as sess:
        for id in movies:
            m_url = f"https://api.themoviedb.org/3/movie/{id}/credits?language=en-US"
            response = sess.get(m_url,
                                headers = headers).json()
            
            for p_id in list(actor['id'] for actor in response['cast']):
                if p_id in list(a['tmdbId'] for a in actors):
                    list(item for item in actors if item['tmdbId'] == p_id)[0]['movie_ids'].append(id)
                else: 
                    p_url = f"https://api.themoviedb.org/3/person/{p_id}?language=en-US"
                    p_response = sess.get(p_url,
                                headers = headers).json()
                    actors.append({
                        'bio': p_response['biography'],
                        'born': p_response['birthday'],
                        'bornIn': p_response['place_of_birth'],
                        'imdbId': p_response['imdb_id'],
                        'name': p_response['name'],
                        'tmdbId': p_id,
                        'url': f'https://www.themoviedb.org/person/{p_id}',
                        'movie_ids': [id,]
                    })

            for p_id in list(director['id'] for director in response['crew'] if director['job'] == 'Director'):
                if p_id in list(d['tmdbId'] for d in directors):
                    list(item for item in directors if item['tmdbId'] == p_id)[0]['movie_ids'].append(id)
                else:
                    p_url = f"https://api.themoviedb.org/3/person/{p_id}?language=en-US"
                    p_response = sess.get(p_url,
                                headers = headers).json()
                    directors.append({
                        'born': p_response['birthday'],
                        'imdbId': p_response['imdb_id'],
                        'name': p_response['name'],
                        'tmdbId': p_id,
                        'url': f'https://www.themoviedb.org/person/{p_id}',
                        'movie_ids': [id,]
                    })

    return (actors, directors)

# Функция для добавления нод с фильмами в БД
def add_movies(movie_data: list[dict],
                DB: str,
                auth: tuple) -> None:
    
    with GraphDatabase.driver(DB, auth = auth) as driver: 
        with driver.session() as session:
            for item in movie_data:
                item_str = ', '.join(f'{key}: "{item[key]}"' for key in item)
                session.run(f'CREATE (m:Movie {{ {item_str} }} )')

# Функция для добавления нод с актерами и связей между актерами и фильмами
def add_people(actor_data: list[dict],
               director_data: list[dict],
               DB: str,
               auth: tuple) -> None:
    
    with GraphDatabase.driver(DB, auth = auth) as driver: 
        with driver.session() as session:
            # Добавляем ноды с актерами:
            for actor in actor_data:
                actor_str = ', '.join(f'{key}: "{actor[key]}"' for key in actor if key != 'movie_ids')
                try:
                    session.run(f'CREATE (a: Person:Actor {{ {actor_str} }} )')
                except: 
                    pass

            # Строим связи c фильмами для актеров
            for actor in actor_data:
                actor_match = f'imdbId: "{actor['imdbId']}"'
                for movie in actor['movie_ids']:
                    movie_match = f'tmdbId: "{movie}"'
                    try:
                        session.run(f'MATCH (m: Movie {{ {movie_match} }}), (a: Person:Actor {{ {actor_match} }}) CREATE (a) - [:ACTED_IN] -> (m)')
                    except:
                        pass

            # Добавляем ноды с режиссерами
            for director in director_data:
                director_str = ', '.join(f'{key}: "{director[key]}"' for key in director if key != 'movie_ids')
                try:
                    session.run(f'CREATE (a: Person:Director {{ {director_str} }} )')
                except:
                    session.run(f'MATCH (director:Person {{tmdbId: {director['tmdbId']} }}) SET director:Director')
                finally:
                    pass

            # Строим связи с фильмами для режиссеров
            for director in director_data:
                director_match = f'imdbId: "{director['imdbId']}"'
                for movie in director['movie_ids']:
                    movie_match = f'tmdbId: "{movie}"'
                    try:
                        session.run(f'MATCH (m: Movie {{ {movie_match} }}), (d: Person:Director {{ {director_match} }}) CREATE (d) - [:DIRECTED] -> (m)')
                    except:
                        pass
#ID фильмов с TMDB
movie_ids = ['43430', # Ирония судьбы
             '56547', # Гараж
             '31139'] # Служебный роман

add_movies(movie_data = get_movie_data(movie_ids),
           DB = r'bolt://localhost:7687',
           auth = DB_AUTH)

actor_data, director_data = get_people_data(movies = movie_ids)
add_people(actor_data = actor_data,
           director_data = director_data,
           DB = r'bolt://localhost:7687',
           auth = DB_AUTH)



