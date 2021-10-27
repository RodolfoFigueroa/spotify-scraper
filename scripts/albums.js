import dotenv from 'dotenv';
dotenv.config();

import { AlbumScraper } from '../scraper.js';

const read_path = './backups/artists_filtered.csv';
const write_path = './albums.csv';

const spotify = new AlbumScraper({
    clientId: process.env.CLIENT_ID,
    clientSecret: process.env.CLIENT_SECRET,
});

async function main() {
    await spotify.login();
    await spotify.init_path(read_path, write_path);
    const promises = [];
    for (let i = 0; i < 10; i++) {
        promises.push(spotify.process_queue());
    }
    await Promise.all(promises);
    console.log('Done');
}

main();