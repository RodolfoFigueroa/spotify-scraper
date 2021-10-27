import dotenv from 'dotenv';
dotenv.config();

import { SongFeaturesScraper } from '../scraper.js';

// const read_path = './backups/albums_filtered.csv';
const read_path = './backups/songs.csv';
const write_path = './song_features.csv';

const spotify = new SongFeaturesScraper({
    clientId: process.env.CLIENT_ID,
    clientSecret: process.env.CLIENT_SECRET,
    concurrency: 10,
});

spotify.login()
    .then(() => {
        spotify.init_path(read_path, write_path);
        spotify.start();
    });
