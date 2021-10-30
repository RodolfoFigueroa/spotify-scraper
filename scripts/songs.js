import dotenv from 'dotenv';
import { MySpotify } from '../scraper.js';
dotenv.config();

const read_path = './files/albums_filtered.csv';
const write_path = './songs.csv';
const headers = ['id', 'name', 'album', 'artists', 'duration_ms', 'explicit', 'disc_number', 'track_number'];

function row_callback(row) {
    return row.id;
}

function after_callback(data) {
    const chunks = [];
    let chunk = [];
    while (data.length > 0) {
        chunk.push(data.pop());
        if (chunk.length >= 20) {
            chunks.push({ data: chunk, chunk: true });
            chunk = [];
        }
    }
    if (chunk.length > 0) {
        chunks.push({ data: chunk, chunk: true });
    }
    return chunks;
}

async function query_callback(popped) {
    if (popped.chunk) {
        return await this.getAlbums(popped.data);
    }
    else {
        return await this.getAlbumTracks(popped.data, { limit: 50, offset: popped.offset });
    }
}

function parse_track(track, album_id) {
    const artists = track.artists.map(x => x.id);
    return [
        track.id,
        track.name,
        album_id,
        artists,
        track.duration_ms,
        track.explicit,
        track.disc_number,
        track.track_number,
    ];
}

async function response_callback(response, popped) {
    const to_push = [], to_write = [];
    if (popped.chunk) {
        response.body.albums.forEach(async album => {
            if (album) {
                album.tracks.items.forEach(track => {
                    to_write.push(parse_track(track, album.id));
                });
                if (album.tracks.next) {
                    to_push.push({ data: album.id, chunk: false, offset: 50 });
                }
            }
        });
    }
    else {
        response.body.items.forEach(track => {
            to_write.push(parse_track(track, popped.data));
        });
        if (response.body.next) {
            to_push.push({ data: popped.data, chunk: false, offset: popped.offset + 50 });
        }
    }
    return [ to_write, to_push ];
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
        {
            read_path: read_path,
            row_callback: row_callback,
            after_callback: after_callback,
            pending: 13050,
        },
    );
    await spotify.process(10);
    console.log('Done');
}

main();