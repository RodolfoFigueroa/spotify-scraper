import dotenv from 'dotenv';
dotenv.config();

import { ArtistScraper } from '../scraper.js';

const path = '../artists.csv';

const spotify = new ArtistScraper({
    clientId: process.env.CLIENT_ID,
    clientSecret: process.env.CLIENT_SECRET,
    concurrency: 10,
});

spotify.login().then(() => {
    spotify.init_path(path).then(() => {
        spotify.start();
    });
});
