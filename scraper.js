import SpotifyWebApi from 'spotify-web-api-node';
import fs from 'fs';
import stringify from 'csv-stringify';
import parse from 'csv-parse/lib/sync.js';

class MySpotify extends SpotifyWebApi {
    constructor(options) {
        super(options);
        this.processed = 0;
        this.write_stream = null;

        this.response_callback = Function.prototype();
        this.query_callback = Function.prototype();
    }

    async login() {
        try {
            const data = await this.clientCredentialsGrant();
            this.setAccessToken(data.body['access_token']);
            this.setRefreshToken(data.body['refresh-token']);
        }
        catch (error) {
            console.log('Couldn\'t login.');
        }
    }

    async refresh() {
        try {
            const data = await this.refreshAccessToken();
            this.setAccessToken(data.body['access_token']);
        }
        catch (error) {
            console.log(error);
        }
    }

    async process_queue() {
        while (this.queue.length > 0) {
            const popped = this.queue.pop();
            let response;
            try {
                response = await this.query_callback(popped);
            }
            catch (error) {
                if (error.statusCode == 429) {
                    const timeout = parseInt(error.headers['retry-after']) * 1000 + 1000;
                    await new Promise(resolve => setTimeout(resolve, timeout));
                }
                else if (error.statusCode == 401) {
                    this.login();
                }
                else {
                    console.log(error);
                }
                this.queue.push(popped);
                continue;
            }

            await this.response_callback(response, popped);
            process.stdout.write(`Processed: ${this.processed}. Pending: ${this.queue.length}\r`);
        }
    }
}

class SongFeaturesScraper extends MySpotify {
    init_path(read_path, write_path) {
        if (!fs.existsSync(read_path)) {
            throw 'ASD';
        }
        let songs = [];
        fs.createReadStream(read_path)
            .pipe(parse({ delimiter: ',', columns: true }))
            .on('data', row => {
                songs.push(row.id);
                if (songs.length == 50) {
                    this.queue.add(async () => this.get_songs_features(songs));
                    songs = [];
                }
            })
            .on('end', () => {
                if (songs.length > 0) {
                    this.queue.add(async () => this.get_songs_features(songs));
                }
            });
        const headers = ['id', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature'];
        const stream = fs.createWriteStream(write_path, { flags: 'a' });
        this.write_stream = stringify({
            columns: headers,
            header: true,
        });
        this.write_stream.pipe(stream);
    }

    async get_songs_features(songs) {
        let response;
        try {
            response = await this.getAudioFeaturesForTracks(songs);
        }
        catch (error) {
            if (error.statusCode == 429) {
                if (this.timeout) {
                    this.timeout.unref();
                }
                await this.queue.pause();
                this.timeout = setTimeout(
                    async () => await this.queue.start(),
                    parseInt(error.headers['retry-after']) * 1000 + 500,
                );
            }
            else if (error.statusCode == 401) {
                await this.queue.pause();
                await this.login();
                await this.queue.start();
            }
            else {
                console.log(error);
            }
            this.queue.add(async () => this.get_songs_features(songs));
            return;
        }

        response.body.audio_features.forEach(song => {
            if (song) {
                this.write_stream.write([
                    song.id,
                    song.danceability,
                    song.energy,
                    song.key,
                    song.loudness,
                    song.mode,
                    song.speechiness,
                    song.acousticness,
                    song.instrumentalness,
                    song.liveness,
                    song.valence,
                    song.tempo,
                    song.time_signature,
                ]);
            }
        });
        this.processed += response.body.audio_features.length;
        process.stdout.write(`Processed: ${this.processed}. Pending: ${this.queue.size}\r`);
    }
}

class SongScraper extends MySpotify {
    init_path(read_path, write_path) {
        if (!fs.existsSync(read_path)) {
            throw 'ASD';
        }
        let albums = [];
        const chunks = [];
        fs.createReadStream(read_path)
            .pipe(parse({ delimiter: ',', columns: true }))
            .on('data', row => {
                albums.push(row.id);
                if (albums.length >= 20) {
                    chunks.push(albums);
                    albums = [];
                }
                process.stdout.write(chunks.length + '\r');
            })
            .on('end', async () => {
                if (albums.length > 0) {
                    chunks.push(albums);
                }
                let chunk;
                while (chunks.length > 0) {
                    chunk = chunks.pop();
                    await this.queue.add(() => this.get_albums_songs(chunk));
                }
            });

        const headers = ['id', 'name', 'album', 'artists', 'duration_ms', 'explicit', 'disc_number', 'track_number'];
        const stream = fs.createWriteStream(write_path, { flags: 'a' });
        this.write_stream = stringify({
            columns: headers,
            header: true,
            cast: { boolean: x => (+x).toString() },
        });
        this.write_stream.pipe(stream);
    }

    write_track(track, album) {
        const artists = track.artists.map(x => x.id);
        this.write_stream.write([
            track.id,
            track.name,
            album,
            artists,
            track.duration_ms,
            track.explicit,
            track.disc_number,
            track.track_number,
        ]);
    }

    async get_album_songs(album, offset) {
        let response;
        try {
            response = await this.getAlbumTracks(album, { limit: 50, offset: offset });
        }
        catch (error) {
            if (error.statusCode == 429) {
                if (this.timeout) {
                    this.timeout.unref();
                }
                await this.queue.pause();
                this.timeout = setTimeout(
                    async () => await this.queue.start(),
                    parseInt(error.headers['retry-after']) * 1000 + 500,
                );
            }
            else if (error.statusCode == 401) {
                await this.queue.pause();
                await this.login();
                await this.queue.start();
            }
            else {
                console.log(error);
            }
            await this.queue.add(async () => this.get_album_songs(album, offset));
            return;
        }
        response.body.items.forEach(track => {
            this.write_track(track, album);
        });
        if (response.body.next) {
            await this.queue.add(async () => this.get_album_songs(album, offset + 50));
        }
        this.processed += response.body.items.length;
        process.stdout.write(`Processed: ${this.processed}. Pending: ${this.queue.size}\r`);
    }

    async get_albums_songs(albums) {
        let response;
        try {
            response = await this.getAlbums(albums);
        }
        catch (error) {
            if (error.statusCode == 429) {
                if (this.timeout) {
                    this.timeout.unref();
                }
                await this.queue.pause();
                this.timeout = setTimeout(
                    async () => await this.queue.start(),
                    parseInt(error.headers['retry-after']) * 1000 + 500,
                );
            }
            else if (error.statusCode == 401) {
                await this.queue.pause();
                await this.login();
                await this.queue.start();
            }
            else {
                console.log(error);
            }
            await this.queue.add(async () => this.get_albums_songs(albums));
            return;
        }

        response.body.albums.forEach(async album => {
            album.tracks.items.forEach(track => {
                this.write_track(track, album.id);
            });
            if (album.tracks.next) {
                await this.queue.add(async () => this.get_album_songs(album.id, 50));
            }
            this.processed += album.tracks.items.length;
        });
        process.stdout.write(`Processed: ${this.processed}. Pending: ${this.queue.size}\r`);
    }
}

class AlbumScraper extends MySpotify {
    constructor(options) {
        super(options);

        this.query_callback = async popped => {
            return await this.getArtistAlbums(
                popped.id,
                { limit: 50, offset: popped.offset, album_type: 'album,single' },
            );
        };

        this.response_callback = async (response, popped) => {
            if (response.body.next) {
                this.queue.push({ id: popped.id, offset: popped.offset + 50 });
            }

            response.body.items.forEach(album => {
                const artists = album.artists.map(x => x.id);
                this.write_stream.write([
                    album.id,
                    album.name,
                    artists,
                    album.album_type,
                    album.release_date,
                    album.total_tracks,
                    album.available_markets,
                ]);
                this.processed++;
            });
        };
    }

    async init_path(read_path, write_path) {
        if (!fs.existsSync(read_path)) {
            throw 'ASD';
        }
        const data = fs.readFileSync(read_path, 'utf8');
        const artists = parse(data, { columns: true });
        this.queue = artists.map(function(artist) {
            return { id: artist.id, offset: 0 };
        });
        console.log('Done reading');

        const headers = ['id', 'name', 'artists', 'type', 'release_date', 'total_tracks', 'markets'];
        const stream = fs.createWriteStream(write_path, { flags: 'a' });
        this.write_stream = stringify({ columns: headers, header: true });
        this.write_stream.pipe(stream);
        console.log('Done piping');
    }
}

class ArtistScraper extends MySpotify {
    constructor(options) {
        super(options);
        this.artists = new Set();
        this.pending_artists = new Set();
    }

    async init_path(path) {
        if (!fs.existsSync(path)) {
            const initial_artist = (await this.getArtist('0qeei9KQnptjwb8MgkqEoy')).body;
            this.queue.add(async () => this.recursive_artists(initial_artist));
        }
        else {
            const tentative_artists = new Set();
            fs.createReadStream(path)
                .pipe(parse({ delimiter: ',' }))
                .on('data', row => {
                    this.artists.add(row[0]);
                    row[4].split(',').forEach(artist => {
                        tentative_artists.add(artist);
                    });
                });
            // const pending_artists = tentative_artists.forEach(artist => {
            //     if (!this.artists.has(artist)) {
            //         0;
            //     }
            // });
        }
        const headers = ['id', 'name', 'followers', 'genres', 'related_artists'];
        const stream = fs.createWriteStream(path, { flags: 'a' });
        this.write_stream = stringify({ columns: headers, header: true });
        this.write_stream.pipe(stream);
    }

    async recursive_artists(artist_data) {
        const root = artist_data.id;
        if (this.artists.has(root)) {
            return;
        }
        this.artists.add(root);

        let response;
        try {
            response = (await this.getArtistRelatedArtists(root)).body.artists;
        }
        catch (error) {
            this.artists.delete(root);
            if (error.statusCode == 429) {
                if (this.timeout) {
                    this.timeout.unref();
                }
                await this.queue.pause();
                this.timeout = setTimeout(
                    async () => await this.queue.start(),
                    parseInt(error.headers['retry-after']) * 1000 + 500,
                );
            }
            else if (error.statusCode == 401) {
                await this.queue.pause();
                await this.login();
                await this.queue.start();
            }
            else {
                console.log(error);
            }
            this.queue.add(async () => this.recursive_artists(artist_data));
            return;
        }

        const found_ids = response.map(artist => artist.id);
        this.write_stream.write([
            artist_data.id,
            artist_data.name,
            artist_data.followers.total,
            artist_data.genres,
            found_ids,
        ]);

        response.forEach(artist => {
            if (!this.artists.has(artist.id) && !this.pending_artists.has(artist.id)) {
                this.queue.add(async () => this.recursive_artists(artist));
                this.pending_artists.add(artist.id);
            }
        });

        this.pending_artists.delete(root);

        this.processed++;
        process.stdout.write(`Processed: ${this.processed}. Pending: ${this.queue.size}\r`);
    }
}

export { MySpotify, AlbumScraper, ArtistScraper, SongScraper, SongFeaturesScraper };