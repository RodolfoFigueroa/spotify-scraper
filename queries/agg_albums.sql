COPY(
	SELECT 
		album,

		duration_ms_avg,
		duration_ms_std,
		quant_duration_ms[1] AS duration_ms_0,
		quant_duration_ms[2] AS duration_ms_25,
		quant_duration_ms[3] AS duration_ms_50,
		quant_duration_ms[4] AS duration_ms_75,
		quant_duration_ms[5] AS duration_ms_100,

		danceability_avg,
		danceability_std,
		quant_danceability[1] AS danceability_0,
		quant_danceability[2] AS danceability_25,
		quant_danceability[3] AS danceability_50,
		quant_danceability[4] AS danceability_75,
		quant_danceability[5] AS danceability_100,

		energy_avg,
		energy_std,
		quant_energy[1] AS energy_0,
		quant_energy[2] AS energy_25,
		quant_energy[3] AS energy_50,
		quant_energy[4] AS energy_75,
		quant_energy[5] AS energy_100,

		loudness_avg,
		loudness_std,
		quant_loudness[1] AS loudness_0,
		quant_loudness[2] AS loudness_25,
		quant_loudness[3] AS loudness_50,
		quant_loudness[4] AS loudness_75,
		quant_loudness[5] AS loudness_100,

		speechiness_avg,
		speechiness_std,
		quant_speechiness[1] AS speechiness_0,
		quant_speechiness[2] AS speechiness_25,
		quant_speechiness[3] AS speechiness_50,
		quant_speechiness[4] AS speechiness_75,
		quant_speechiness[5] AS speechiness_100,

		acousticness_avg,
		acousticness_std,
		quant_acousticness[1] AS acousticness_0,
		quant_acousticness[2] AS acousticness_25,
		quant_acousticness[3] AS acousticness_50,
		quant_acousticness[4] AS acousticness_75,
		quant_acousticness[5] AS acousticness_100,

		instrumentalness_avg,
		instrumentalness_std,
		quant_instrumentalness[1] AS instrumentalness_0,
		quant_instrumentalness[2] AS instrumentalness_25,
		quant_instrumentalness[3] AS instrumentalness_50,
		quant_instrumentalness[4] AS instrumentalness_75,
		quant_instrumentalness[5] AS instrumentalness_100,

		liveness_avg,
		liveness_std,
		quant_liveness[1] AS liveness_0,
		quant_liveness[2] AS liveness_25,
		quant_liveness[3] AS liveness_50,
		quant_liveness[4] AS liveness_75,
		quant_liveness[5] AS liveness_100,

		valence_avg,
		valence_std,
		quant_valence[1] AS valence_0,
		quant_valence[2] AS valence_25,
		quant_valence[3] AS valence_50,
		quant_valence[4] AS valence_75,
		quant_valence[5] AS valence_100,

		tempo_avg,
		tempo_std,
		quant_tempo[1] AS tempo_0,
		quant_tempo[2] AS tempo_25,
		quant_tempo[3] AS tempo_50,
		quant_tempo[4] AS tempo_75,
		quant_tempo[5] AS tempo_100

		FROM(
			SELECT 
				album, 
				AVG(duration_ms) AS duration_ms_avg,
				AVG(danceability) AS danceability_avg,
				AVG(energy) AS energy_avg,
				AVG(loudness) AS loudness_avg,
				AVG(speechiness) AS speechiness_avg,
				AVG(acousticness) AS acousticness_avg,
				AVG(instrumentalness) AS instrumentalness_avg,
				AVG(liveness) AS liveness_avg,
				AVG(valence) AS valence_avg,
				AVG(tempo) AS tempo_avg,
			
				STDDEV_POP(duration_ms) AS duration_ms_std,
				STDDEV_POP(danceability) AS danceability_std,
				STDDEV_POP(energy) AS energy_std,
				STDDEV_POP(loudness) AS loudness_std,
				STDDEV_POP(speechiness) AS speechiness_std,
				STDDEV_POP(acousticness) AS acousticness_std,
				STDDEV_POP(instrumentalness) AS instrumentalness_std,
				STDDEV_POP(liveness) AS liveness_std,
				STDDEV_POP(valence) AS valence_std,
				STDDEV_POP(tempo) AS tempo_std,
			
				PERCENTILE_CONT(ARRAY [0, 0.25, 0.5, 0.75, 1]) WITHIN GROUP (ORDER BY duration_ms) AS quant_duration_ms,
				PERCENTILE_CONT(ARRAY [0, 0.25, 0.5, 0.75, 1]) WITHIN GROUP (ORDER BY danceability) AS quant_danceability,
				PERCENTILE_CONT(ARRAY [0, 0.25, 0.5, 0.75, 1]) WITHIN GROUP (ORDER BY energy) AS quant_energy,
				PERCENTILE_CONT(ARRAY [0, 0.25, 0.5, 0.75, 1]) WITHIN GROUP (ORDER BY loudness) AS quant_loudness,
				PERCENTILE_CONT(ARRAY [0, 0.25, 0.5, 0.75, 1]) WITHIN GROUP (ORDER BY speechiness) AS quant_speechiness,
				PERCENTILE_CONT(ARRAY [0, 0.25, 0.5, 0.75, 1]) WITHIN GROUP (ORDER BY acousticness) AS quant_acousticness,
				PERCENTILE_CONT(ARRAY [0, 0.25, 0.5, 0.75, 1]) WITHIN GROUP (ORDER BY instrumentalness) AS quant_instrumentalness,
				PERCENTILE_CONT(ARRAY [0, 0.25, 0.5, 0.75, 1]) WITHIN GROUP (ORDER BY liveness) AS quant_liveness,
				PERCENTILE_CONT(ARRAY [0, 0.25, 0.5, 0.75, 1]) WITHIN GROUP (ORDER BY valence) AS quant_valence,
				PERCENTILE_CONT(ARRAY [0, 0.25, 0.5, 0.75, 1]) WITHIN GROUP (ORDER BY tempo) AS quant_tempo
			FROM songs
			GROUP BY album
	) AS t1
) TO '/media/postgres2/albums_aggregated.csv' CSV HEADER