import requests
from bs4 import BeautifulSoup
from collections import defaultdict
from statistics import mean
import csv
import concurrent.futures
import functools

# Replace with your actual TMDB API key
TMDB_API_KEY = ""

# In-memory cache for TMDB API results
tmdb_cache = {}

@functools.lru_cache(maxsize=10000)
def fetch_tmdb_data(movie_title):
    if movie_title in tmdb_cache:
        return tmdb_cache[movie_title]
    
    base_url = "https://api.themoviedb.org/3"
    search_url = f"{base_url}/search/movie?api_key={TMDB_API_KEY}&query={movie_title}"
    
    response = requests.get(search_url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            movie_id = data['results'][0]['id']
            details_url = f"{base_url}/movie/{movie_id}?api_key={TMDB_API_KEY}"
            details_response = requests.get(details_url)
            if details_response.status_code == 200:
                result = details_response.json()
                tmdb_cache[movie_title] = result
                return result
    return None

def convert_rating_to_number(rating_text):
    if rating_text is None:
        return None
    full_stars = rating_text.count('★')
    half_star = 0.5 if '½' in rating_text else 0
    return (full_stars + half_star) * 2

def get_user_rated_movies(username, max_pages=10):
    movies = []
    base_url = f"https://letterboxd.com/{username}/films/ratings/"
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(requests.get, f"{base_url}page/{page}/"): page for page in range(1, max_pages + 1)}
        for future in concurrent.futures.as_completed(future_to_url):
            response = future.result()
            content = response.text
            
            if not content:
                break
            
            soup = BeautifulSoup(content, 'html.parser')
            
            if soup.find('div', class_='error-message'):
                break
            
            film_list = soup.find('ul', class_='poster-list') or \
                        soup.find('ul', class_='film-list') or \
                        soup.find('ul', class_='rated-film-list')
            
            if not film_list:
                break
            
            for film in film_list.find_all('li'):
                film_data = film.find('div', class_='film-poster') or film.find('div', class_='poster')
                if film_data:
                    film_slug = film_data.get('data-film-slug') or film_data.get('data-target-link')
                    if film_slug:
                        film_title = film_slug.split('/')[-1].replace('-', ' ').title()
                        film_title = ' '.join(film_title.split()[:-1]) if film_title.split()[-1].isdigit() else film_title
                        
                        rating_elem = film.find('span', class_='rating') or film.find('span', class_='rated-rating')
                        user_rating = convert_rating_to_number(rating_elem.text if rating_elem else None)
                        
                        movies.append({
                            'title': film_title,
                            'rating': user_rating,
                        })

    movies = [movie for movie in movies if movie['rating'] is not None]
    
    # Fetch TMDB data concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_movie = {executor.submit(fetch_tmdb_data, movie['title']): movie for movie in movies}
        for future in concurrent.futures.as_completed(future_to_movie):
            movie = future_to_movie[future]
            tmdb_data = future.result()
            if tmdb_data:
                movie['genres'] = set(genre['name'] for genre in tmdb_data.get('genres', []))
            else:
                movie['genres'] = set()

    return movies

def create_user_profile(rated_movies):
    genre_ratings = defaultdict(list)
    genre_counts = defaultdict(int)
    for movie in rated_movies:
        if movie['rating'] is not None:
            for genre in movie['genres']:
                genre_ratings[genre].append(movie['rating'])
                genre_counts[genre] += 1
    
    return {
        genre: {
            'score': mean(ratings),
            'count': genre_counts[genre]
        }
        for genre, ratings in genre_ratings.items()
    }

def calculate_average_rating(rated_movies):
    ratings = [movie['rating'] for movie in rated_movies if movie['rating'] is not None]
    return mean(ratings) if ratings else 0

def calculate_relative_scores(user_profile, average_rating):
    max_count = max(data['count'] for data in user_profile.values()) if user_profile else 1
    return {
        genre: {
            'score': data['score'] / average_rating,
            'weight': 0.5 + 0.5 * (data['count'] / max_count) ** 0.5
        }
        for genre, data in user_profile.items()
    }

def read_final_ranked_movies(file_path):
    movies = []
    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            movies.append({
                'title': row['title'],
                'genres': set(row['genres'].split(', ')),
                'final_score': float(row['final_score'])
            })
    return movies

def get_watched_movies(rated_movies):
    return {normalize_title(movie['title']) for movie in rated_movies}

def normalize_title(title):
    return title.lower().replace(':', '').replace('-', ' ').strip()

def recommend_movies(user_profile, average_rating, final_ranked_movies, watched_movies, top_n=25):
    relative_scores = calculate_relative_scores(user_profile, average_rating)
    
    recommended_movies = []
    for movie in final_ranked_movies:
        normalized_title = normalize_title(movie['title'])
        if normalized_title not in watched_movies:
            adjusted_score = movie['final_score']
            for i, genre in enumerate(sorted(movie['genres'])):
                weight = 1 / (i + 1)
                dampened_weight = 1 + (weight - 1) * 0.2
                if genre in relative_scores:
                    genre_score = relative_scores[genre]['score']
                    genre_weight = relative_scores[genre]['weight']
                    if genre_score >= 1:
                        adjusted_score /= (2 * genre_score) ** (dampened_weight * (0.5 + genre_weight))
                    else:
                        adjusted_score /= (0.5 * genre_score) ** (dampened_weight * (0.5 + genre_weight))
                else:
                    adjusted_score *= 2 ** dampened_weight
            movie['adjusted_score'] = round(adjusted_score, 4)
            recommended_movies.append(movie)
    
    return sorted(recommended_movies, key=lambda x: x['adjusted_score'])[:top_n]

def main():
    username = input("Enter a Letterboxd username: ")
    rated_movies = get_user_rated_movies(username)
    
    if not rated_movies:
        print(f"No rated movies found for user {username}.")
        return

    average_rating = calculate_average_rating(rated_movies)
    print(f"\nUser has rated {len(rated_movies)} movies.")
    print(f"Average rating: {average_rating:.2f}")

    user_profile = create_user_profile(rated_movies)
    
    print("\nUser Genre Profile:")
    for genre, data in sorted(user_profile.items(), key=lambda x: x[1]['score'], reverse=True):
        print(f"{genre}: Score: {data['score']:.2f}, Count: {data['count']}")

    watched_movies = get_watched_movies(rated_movies)
    final_ranked_movies = read_final_ranked_movies('final_ranked_movies.csv')
    recommended_movies = recommend_movies(user_profile, average_rating, final_ranked_movies, watched_movies, top_n=25)
    print("\nTop 25 Recommended Movies (excluding watched movies):")
    for i, movie in enumerate(recommended_movies, 1):
        print(f"{i}. {movie['title']} (Adjusted Score: {movie['adjusted_score']:.2f}, Genres: {', '.join(movie['genres'])})")

if __name__ == "__main__":
    main()
