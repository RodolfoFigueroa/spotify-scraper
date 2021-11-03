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
        const { read_path, row_callback, after_callback, pending, queue } = options;
        if (read_path && queue) {
            throw 'Only one of read_path and queue should be nonempty.';
        }
        else if (read_path) {
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
        else if (queue) {
            this.queue = queue;
        }
        else {
            throw 'One of read_path and init_callback should be nonempty';
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
        let flag = true;
        for (let i = 0; i < 3; i++) {
            if (this.queue.length > 0) {
                flag = false;
                break;
            }
            else {
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
        if (flag) {
            console.log('asd');
            return;
        }
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
                    console.log('Error');
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
        console.log('asdd');
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

export { MySpotify };