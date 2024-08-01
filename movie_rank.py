import csv
from collections import defaultdict

def load_csv(filename):
    movies = {}
    with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            title = row['title']
            movies[title] = row
    return movies

def calculate_final_score(popularity_rank, rating_rank, max_rank):
    if popularity_rank is None:
        popularity_rank = max_rank
    if rating_rank is None:
        rating_rank = max_rank
    return 0.25 * float(popularity_rank) + 0.75 * float(rating_rank)

def main():
    popular_movies = load_csv('top_2500_movies_popular.csv')
    rated_movies = load_csv('top_2500_movies_rating.csv')

    all_movies = defaultdict(dict)
    max_rank = max(len(popular_movies), len(rated_movies))

    # Combine data from both files
    for title in set(popular_movies.keys()) | set(rated_movies.keys()):
        if title in popular_movies:
            all_movies[title]['popularity_rank'] = popular_movies[title]['popularity_rank']
        else:
            all_movies[title]['popularity_rank'] = None

        if title in rated_movies:
            all_movies[title]['rating_rank'] = rated_movies[title]['rating_rank']
        else:
            all_movies[title]['rating_rank'] = None

        all_movies[title]['title'] = title
        all_movies[title]['year'] = popular_movies.get(title, rated_movies.get(title))['year']
        all_movies[title]['genres'] = popular_movies.get(title, rated_movies.get(title))['genres']

    # Calculate final score and rank
    for movie in all_movies.values():
        movie['final_score'] = calculate_final_score(movie['popularity_rank'], movie['rating_rank'], max_rank)

    # Sort movies by final score
    ranked_movies = sorted(all_movies.values(), key=lambda x: x['final_score'])

    # Assign final rank
    for i, movie in enumerate(ranked_movies, 1):
        movie['final_rank'] = i

    # Save to CSV
    output_filename = 'final_ranked_movies.csv'
    with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['final_rank', 'title', 'year', 'genres', 'popularity_rank', 'rating_rank', 'final_score']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for movie in ranked_movies:
            writer.writerow({
                'final_rank': movie['final_rank'],
                'title': movie['title'],
                'year': movie['year'],
                'genres': movie['genres'],
                'popularity_rank': movie['popularity_rank'] if movie['popularity_rank'] is not None else 'N/A',
                'rating_rank': movie['rating_rank'] if movie['rating_rank'] is not None else 'N/A',
                'final_score': f"{movie['final_score']:.2f}"
            })

    print(f"Final ranking saved to {output_filename}")
    print("\nTop 10 movies in the final ranking:")
    for movie in ranked_movies[:10]:
        print(f"{movie['final_rank']}. {movie['title']} ({movie['year']}) - Score: {movie['final_score']:.2f}")

if __name__ == "__main__":
    main()
