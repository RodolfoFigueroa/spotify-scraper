import dotenv from 'dotenv';
import { MySpotify } from '../scraper.js';
dotenv.config();

const read_path = './files/songs_filtered.csv';
const write_path = './song_features.csv';
const headers = ['id', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature'];

function row_callback(row) {
    return row.id;
}

function after_callback(data) {
    const chunks = [];
    let chunk = [];
    while (data.length > 0) {
        chunk.push(data.pop());
        if (chunk.length >= 50) {
            chunks.push(chunk);
            chunk = [];
        }
    }
    if (chunk.length > 0) {
        chunks.push(chunk);
    }
    return chunks;
}

async function query_callback(popped) {
    return await this.getAudioFeaturesForTracks(popped);
}

async function response_callback(response) {
    const to_push = [], to_write = [];
    response.body.audio_features.forEach(song => {
        if (song) {
            to_write.push([
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
        },
    );
    await spotify.process(10);
    console.log('Done');
}

main();