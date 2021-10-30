import dotenv from 'dotenv';
import { MySpotify } from '../scraper.js';
dotenv.config();

const write_path = './artists2.csv';
const headers = ['id', 'name', 'followers', 'genres', 'related_artists'];
const seen_artists = new Set();
const pending_artists = new Set();

async function query_callback(popped) {
    
    return await this.getArtistRelatedArtists(popped);
}

async function response_callback(response, popped) {
    const related_ids = [];
    const to_write = [], to_push = [];

    response.body.artists.forEach(artist => {
        related_ids.push(artist.id);
        if (!seen_artists.has(artist.id) && !pending_artists.has(artist.id)) {
            to_push.push(artist);
        }
    });

    to_write.push([
        popped.id,
        popped.name,
        popped.followers.total,
        popped.genres,
        related_ids,
    ]);
    return [to_write, to_push];
}

const spotify = new MySpotify(
    query_callback,
    response_callback,
    {
        clientId: process.env.CLIENT_ID,
        clientSecret: process.env.CLIENT_SECRET,
    },
);


async function main() {
    await spotify.login();
    await spotify.init_path(
        write_path,
        headers,
    );
    await spotify.process(10);
    console.log('Done');
}

main();