import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import csv
import time
import os
import concurrent.futures
from functools import lru_cache

# Replace with your actual TMDB API key
TMDB_API_KEY = ""

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    return webdriver.Chrome(options=chrome_options)

def get_letterboxd_popular_movies(driver, num_movies=2500):
    url = "https://letterboxd.com/films/popular/"
    driver.get(url)

    movies = []
    page = 1
    while len(movies) < num_movies:
        try:
            print(f"Fetching page {page}...")
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "poster-container")))
            film_list = driver.find_elements(By.CSS_SELECTOR, "li.poster-container")
            
            for film in film_list:
                if len(movies) >= num_movies:
                    break
                
                film_data = {}
                film_link = film.find_element(By.CSS_SELECTOR, "div.film-poster")
                
                title_year = film_link.get_attribute("data-film-name")
                if '(' in title_year and ')' in title_year:
                    film_data['title'] = title_year[:title_year.rfind('(')].strip()
                    film_data['year'] = title_year[title_year.rfind('(')+1:title_year.rfind(')')]
                else:
                    film_data['title'] = title_year
                    film_data['year'] = ''
                
                film_data['popularity_rank'] = len(movies) + 1
                movies.append(film_data)
            
            print(f"Collected {len(movies)} movies so far...")
            
            if len(movies) < num_movies:
                next_button = driver.find_element(By.CSS_SELECTOR, "a.next")
                if next_button:
                    driver.execute_script("arguments[0].click();", next_button)
                    time.sleep(2)  # Wait for the next page to load
                    page += 1
                else:
                    print("No more pages available.")
                    break
        except Exception as e:
            print(f"Error fetching movies on page {page}: {e}")
            break

    return movies[:num_movies]

@lru_cache(maxsize=10000)
def get_tmdb_genres(title, year):
    search_url = f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_API_KEY}&query={title}&year={year}"
    try:
        response = requests.get(search_url)
        if response.status_code == 200:
            results = response.json().get('results', [])
            if results:
                movie_id = results[0]['id']
                details_url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMDB_API_KEY}"
                details_response = requests.get(details_url)
                if details_response.status_code == 200:
                    details = details_response.json()
                    return [genre['name'] for genre in details.get('genres', [])]
    except Exception as e:
        print(f"Error fetching TMDb genres for {title} ({year}): {e}")
    return []

def fetch_genres_concurrent(movies):
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_movie = {executor.submit(get_tmdb_genres, movie['title'], movie['year']): movie for movie in movies}
        for future in concurrent.futures.as_completed(future_to_movie):
            movie = future_to_movie[future]
            try:
                genres = future.result()
                movie['genres'] = genres
                print(f"Fetched genres for {movie['title']}: {', '.join(genres)}")
            except Exception as exc:
                print(f"Error fetching genres for {movie['title']}: {exc}")

def save_to_csv(movies, filename='top_2500_movies_popular.csv'):
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['popularity_rank', 'title', 'year', 'genres']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows({
                'popularity_rank': movie['popularity_rank'],
                'title': movie['title'],
                'year': movie['year'],
                'genres': ', '.join(movie['genres'])
            } for movie in movies)
        print(f"Saved {len(movies)} movies to {filename}")
        print(f"File saved at: {os.path.abspath(filename)}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def main():
    driver = setup_driver()
    try:
        print("Fetching popular movies from Letterboxd...")
        start_time = time.time()
        movies = get_letterboxd_popular_movies(driver, 2500)
        print(f"Fetched {len(movies)} movies from Letterboxd in {time.time() - start_time:.2f} seconds")

        print("Fetching genres from TMDB...")
        start_time = time.time()
        fetch_genres_concurrent(movies)
        print(f"Fetched all genres in {time.time() - start_time:.2f} seconds")

        save_to_csv(movies)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
