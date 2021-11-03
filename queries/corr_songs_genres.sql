COPY (
	SELECT DISTINCT
		song, 
		name AS genre 
	FROM artists_songs
	INNER JOIN artists_genres
		ON artists_songs.artist = artists_genres.artist
	INNER JOIN genres
		ON genres.id = artists_genres.genre
) TO '/media/postgres2/songs_genres.csv' CSV HEADER