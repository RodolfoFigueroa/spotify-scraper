import dotenv from 'dotenv';
import { MySpotify } from '../scraper.js';
dotenv.config();

const read_path = './files/artists_filtered.csv';
const write_path = './albums.csv';
const headers = ['id', 'name', 'artists', 'type', 'release_date', 'total_tracks', 'markets'];

function row_callback(row) {
    return { id: row.id, offset: 0 };
}

async function query_callback(popped) {
    return await this.getArtistAlbums(
        popped.id,
        { limit: 50, offset: popped.offset, album_type: 'album,single' },
    );
}

async function response_callback(response, popped) {
    let to_push = [];
    if (response.body.next) {
        to_push = [{ id: popped.id, offset: popped.offset + 50 }];
    }

    const to_write = [];
    response.body.items.forEach(album => {
        const artists = album.artists.map(x => x.id);
        to_write.push([
            album.id,
            album.name,
            artists,
            album.album_type,
            album.release_date,
            album.total_tracks,
            album.available_markets,
        ]);
    });
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
        { read_path: read_path, row_callback: row_callback },
    );
    await spotify.process(10);
    console.log('Done');
}

main();