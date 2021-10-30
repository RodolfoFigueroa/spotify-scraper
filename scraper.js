import SpotifyWebApi from 'spotify-web-api-node';
import fs from 'fs';
import stringify from 'csv-stringify';
import parse from 'csv-parse';

class MySpotify extends SpotifyWebApi {
    constructor(query_callback, response_callback, options) {
        super(options);
        this.processed = 0;
        this.write_stream = null;

        this.response_callback = response_callback;
        this.query_callback = query_callback;
    }

    async login() {
        const data = await this.clientCredentialsGrant();
        this.setAccessToken(data.body['access_token']);
        this.setRefreshToken(data.body['refresh-token']);
    }

    async init_path(write_path, headers, options = null) {
        const { read_path, row_callback, after_callback, pending } = options;
        if (read_path) {
            if (!fs.existsSync(read_path)) {
                throw 'Read file doesn\'t exist.';
            }
            const rows = [];
            await new Promise((resolve, reject) => {
                fs.createReadStream(read_path)
                    .pipe(parse({ delimiter: ',', columns: true }))
                    .on('data', row => {
                        rows.push(row_callback(row));
                    })
                    .on('end', resolve)
                    .on('error', reject);
            });

            if (after_callback) {
                this.queue = after_callback(rows);
            }
            else {
                this.queue = rows;
            }
            if (pending) {
                this.queue = this.queue.slice(0, pending);
            }
        }

        const stream = fs.createWriteStream(write_path, { flags: 'a' });
        this.write_stream = stringify({
            columns: headers,
            header: true,
            cast: { boolean: x => (+x).toString() },
        });
        this.write_stream.pipe(stream);
    }

    async consume_queue() {
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
                else if (error.statusCode == 400) {
                    console.log(popped);
                    console.log(error.body);
                }
                else {
                    console.log(error);
                }
                this.queue.push(popped);
                continue;
            }

            if (!response) {
                continue;
            }

            const [ to_write, to_push ] = await this.response_callback(response, popped);
            to_write.forEach(row => this.write_stream.write(row));
            to_push.forEach(item => this.queue.push(item));

            this.processed += to_write.length;
            process.stdout.write(`Written: ${this.processed}. Pending: ${this.queue.length}\r`);
        }
    }

    async process(n) {
        const promises = [];
        for (let i = 0; i < n; i++) {
            promises.push(this.consume_queue());
        }
        try {
            await Promise.all(promises);
        }
        catch (error) {
            console.log(error);
            console.log(`Processed: ${this.processed}. Pending: ${this.queue.length}`);
        }
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

export { MySpotify, ArtistScraper };