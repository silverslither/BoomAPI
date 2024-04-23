const resetCondition = (prev, cur) => prev > 2600 && cur < 2550;

const NUM_REPLAYS = 20;
const NUM_LB_PLAYERS = 50;

const LEN_REPLAYS = NUM_REPLAYS * 168;
const LEN_LB_PLAYERS = NUM_LB_PLAYERS * 68;

let settingsHash = "00000000";

const REQUESTS = {
    get login() {
        return `3c0000006f00000002000000${settingsHash}${Buffer.from(DEVICE_ID).toString("hex")}00000000000000000000000000000000000000000000000000000000`;
    },
    replays_leaderboard: "0c0000007900000003000000" + "0c000000d200000001000000"
};

const INT4_MAX = 2 ** 31 - 1;

import { EXPRESS_PORT, POLLINTERVAL_MS, TIMEOUT_MS, DEVICE_ID, PG_CONFIG, SAMPLE_PERIODS, CURRENT_SAMPLE_PERIOD } from "./env.js";
import { cards } from "./cards.js";

import net from "net";
import pg from "pg";
const sqlPool = new pg.Pool(PG_CONFIG);

import fs from "fs";

import express from "express";
const api = express();

import cors from "cors";
api.use(cors());

api.get("/gamecount", async (req, res) => {
    try {
        const table = SAMPLE_PERIODS.gamecount.includes(req.query.table) ? req.query.table : CURRENT_SAMPLE_PERIOD.ladder;
        const response = await sqlPool.query({
            text: "SELECT count FROM gamecount WHERE id = $1",
            values: [table]
        });
        res.json(response.rows[0] ?? null);
    } catch (err) {
        res.status(500).end();
        console.error(err);
    }
});

api.get("/player", async (req, res) => {
    try {
        const table = SAMPLE_PERIODS.players.includes(req.query.table) ? req.query.table : CURRENT_SAMPLE_PERIOD.ladder;
        const id = Number(req.query.id);
        if (id !== id || id < 0 || id > INT4_MAX) {
            res.status(400).end();
            return;
        }
        const response = await sqlPool.query({
            text: `SELECT * FROM players${table} WHERE id = $1`,
            values: [id]
        });
        if (response.rows[0]?.decks != null)
            response.rows[0].decks = parsePlayerDecks(response.rows[0].decks);
        res.json(response.rows[0] ?? null);
    } catch (err) {
        res.status(500).end();
        console.error(err);
    }
});

api.get("/playersearch", async (req, res) => {
    try {
        const table = SAMPLE_PERIODS.players.includes(req.query.table) ? req.query.table : CURRENT_SAMPLE_PERIOD.ladder;
        if (!req.query.query) {
            res.status(400).end();
            return;
        }
        const response = await sqlPool.query({
            text: `SELECT id, name, medals FROM players${table} WHERE position($1 in LOWER(name)) > 0 LIMIT 20`,
            values: [req.query.query.toLowerCase()]
        });
        res.json(response.rows);
    } catch (err) {
        res.status(500).end();
        console.error(err);
    }
});

api.get("/previewplayers", async (req, res) => {
    try {
        const table = SAMPLE_PERIODS.players.includes(req.query.table) ? req.query.table : CURRENT_SAMPLE_PERIOD.ladder;
        if (!req.query.ids) {
            res.status(400).end();
            return;
        }
        let ids;
        try {
            ids = JSON.parse(req.query.ids);
        } catch (err) {
            res.status(400).end();
            return;
        }
        if (!ids.every(v => typeof v === "number" && v >= 0 && v <= INT4_MAX)) {
            res.status(400).end();
            return;
        }
        const response = await sqlPool.query({
            text: `SELECT t.id, name, medals FROM players${table} t JOIN unnest($1::int4[]) WITH ORDINALITY AS p(id, o) ON p.id = t.id ORDER BY p.o`,
            values: [ids]
        });
        res.json(response.rows);
    } catch (err) {
        res.status(500).end();
        console.error(err);
    }
});

api.get("/leaderboard", async (req, res) => {
    try {
        const table = SAMPLE_PERIODS.leaderboard.includes(req.query.table) ? req.query.table : CURRENT_SAMPLE_PERIOD.ladder;
        const response = await sqlPool.query(`SELECT * FROM leaderboard${table} ORDER BY RANK ASC`);
        res.json(response.rows);
    } catch (err) {
        res.status(500).end();
        console.error(err);
    }
});

api.get("/game", async (req, res) => {
    try {
        const table = SAMPLE_PERIODS.players.includes(req.query.table) ? req.query.table : CURRENT_SAMPLE_PERIOD.ladder;
        const id = Number(req.query.id);
        if (id !== id || id < 0 || id > INT4_MAX) {
            res.status(400).end();
            return;
        }
        const response = await sqlPool.query({
            text: `SELECT * FROM games${table} WHERE id = $1`,
            values: [id]
        });
        res.json(response.rows[0] ?? null);
    } catch (err) {
        res.status(500).end();
        console.error(err);
    }
});

api.get("/games", async (req, res) => {
    try {
        const table = SAMPLE_PERIODS.players.includes(req.query.table) ? req.query.table : CURRENT_SAMPLE_PERIOD.ladder;
        if (!req.query.ids) {
            res.status(400).end();
            return;
        }
        let ids;
        try {
            ids = JSON.parse(req.query.ids);
        } catch (err) {
            res.status(400).end();
            return;
        }
        if (!ids.every(v => typeof v === "number" && v >= 0 && v <= INT4_MAX)) {
            res.status(400).end();
            return;
        }
        const response = await sqlPool.query({
            text: `SELECT t.* FROM games${table} t JOIN unnest($1::int4[]) WITH ORDINALITY AS g(id, o) ON g.id = t.id ORDER BY g.o`,
            values: [ids]
        });
        res.json(response.rows);
    } catch (err) {
        res.status(500).end();
        console.error(err);
    }
});

api.get("/deck", async (req, res) => {
    try {
        const table = SAMPLE_PERIODS.decks.includes(req.query.table) ? req.query.table : CURRENT_SAMPLE_PERIOD.ladder;
        if (!req.query.id || req.query.id.length !== 16) {
            res.status(400).end();
            return;
        }
        const response = await sqlPool.query({
            text: `SELECT * FROM decks${table} WHERE id = $1`,
            values: [req.query.id]
        });
        if (response.rows[0] != null)
            response.rows[0].card_matchups = parseCardMatchups(response.rows[0].card_matchups);
        res.json(response.rows[0] ?? null);
    } catch (err) {
        res.status(500).end();
        console.error(err);
    }
});

api.get("/topdecks", async (req, res) => {
    try {
        const table = SAMPLE_PERIODS.decks.includes(req.query.table) ? req.query.table : CURRENT_SAMPLE_PERIOD.ladder;
        const sort = (req.query.sort ?? "USE").toUpperCase();
        switch (sort) {
            case "USE": {
                const response = await sqlPool.query(`SELECT id, wins, losses, draws, rating FROM decks${table} ORDER BY wins + losses + draws DESC LIMIT 20`);
                res.json(response.rows);
                break;
            }
            case "WIN": {
                const response = await sqlPool.query(`SELECT id, wins, losses, draws, rating FROM decks${table} ORDER BY wins::float8 / greatest(losses + draws, 0.5) DESC LIMIT 20`);
                res.json(response.rows);
                break;
            }
            case "RATING": {
                const response = await sqlPool.query(`SELECT id, wins, losses, draws, rating FROM decks${table} WHERE wins + losses + draws > 19 ORDER BY rating DESC LIMIT 20`);
                res.json(response.rows);
                break;
            }
            default:
                res.status(400).end();
        }
    } catch (err) {
        res.status(500).end();
        console.error(err);
    }
});

api.get("/decksearch", async (req, res) => {
    try {
        const table = SAMPLE_PERIODS.decks.includes(req.query.table) ? req.query.table : CURRENT_SAMPLE_PERIOD.ladder;
        if (!req.query.query) {
            res.status(400).end();
            return;
        }
        let cards;
        try {
            cards = JSON.parse(req.query.query);
            cards = [...new Set(cards)];
        } catch (err) {
            res.status(400).end();
            return;
        }
        if (cards.length > 8 || !cards.every(v => typeof v === "number" && v >= 0 && v <= 255)) {
            res.status(400).end();
            return;
        }
        const response = await sqlPool.query({
            text: `SELECT id, wins, losses, draws, rating FROM decks${table} WHERE match_ba_deck(id::text, $1::int4[]) ORDER BY wins + losses + draws DESC LIMIT 20`,
            values: [cards]
        });
        res.json(response.rows);
    } catch (err) {
        res.status(500).end();
        console.error(err);
    }
});

api.get("/card", async (req, res) => {
    try {
        const table = SAMPLE_PERIODS.cards.includes(req.query.table) ? req.query.table : CURRENT_SAMPLE_PERIOD.ladder;
        const id = Number(req.query.id);
        if (id !== id || id < 0 || id > INT4_MAX) {
            res.status(400).end();
            return;
        }
        const response = await sqlPool.query({
            text: `SELECT * FROM cards${table} WHERE id = $1`,
            values: [id]
        });
        if (response.rows[0] != null)
            response.rows[0].card_matchups = parseCardMatchups(response.rows[0].card_matchups);
        res.json(response.rows[0] ?? null);
    } catch (err) {
        res.status(500).end();

    }
});

api.get("/cards", async (req, res) => {
    try {
        const table = SAMPLE_PERIODS.cards.includes(req.query.table) ? req.query.table : CURRENT_SAMPLE_PERIOD.ladder;
        const sort = (req.query.sort ?? "USE").toUpperCase();
        switch (sort) {
            case "USE": {
                const response = await sqlPool.query(`SELECT id, wins, losses, draws, rating FROM cards${table} ORDER BY wins + losses + draws DESC`);
                res.json(response.rows);
                break;
            }
            case "WIN": {
                const response = await sqlPool.query(`SELECT id, wins, losses, draws, rating FROM cards${table} ORDER BY wins::float8 / greatest(losses + draws, 0.5) DESC`);
                res.json(response.rows);
                break;
            }
            case "RATING": {
                const response = await sqlPool.query(`SELECT id, wins, losses, draws, rating FROM cards${table} WHERE wins + losses + draws > 19 ORDER BY rating DESC`);
                res.json(response.rows);
                break;
            }
            default:
                res.status(400).end();
        }
    } catch (err) {
        res.status(500).end();
        console.error(err);
    }
});

api.get("/settings/*", (req, res) => {
    const path = `.${req.path}`;

    if (!fs.existsSync(path) || fs.statSync(path).isDirectory()) {
        res.status(404).send("404 Not Found");
        return;
    }

    res.sendFile(path, { root: "." });
});

api.get("*", (_req, res) => {
    res.status(404).send("404 Not Found");
});

await initSql();

const server = api.listen(EXPRESS_PORT, () => {
    const port = server.address().port;
    console.log(`Running webserver on port ${port}.`);
    query();
});

async function query() {
    const server = await (await fetch(`http://circlebox.net/boom_arena/api/api_auto_update.php?${new URLSearchParams({
        Mode: 0,
        Os: 2,
        DeviceId: DEVICE_ID,
        Lang: "english",
        ClientVersion: "",
        Time: Date.now()
    })}`)).json();

    let HOST = server.ServerAddress;
    let PORT = server.ServerPort;

    let failcount = 1;
    let topmedals = 0;

    while (true) { // eslint-disable-line
        try {
            const response = await getResponse(PORT, HOST);
            const data = parseResponse(response);
            if (data == null) {
                switch (failcount++) {
                    case 0:
                        console.error(`warning: unable to parse response, retrying requests at ${new Date().toLocaleString("en-CA")}`);
                        fs.writeFileSync("invalid_response_log.bin", response);
                        continue;
                    case 1: {
                        console.error(`warning: unable to parse response, trying settings hash update at ${new Date().toLocaleString("en-CA")}`);
                        const settings = parseSettings(await getSettings(PORT, HOST));

                        let hash = 0;
                        for (const i in settings) {
                            hash += settingsHashCode(settings[i]);
                            fs.writeFileSync(`settings/${i.split("_")[1]}.csv`, settings[i].join("\n"));
                        }

                        const tmpbuf = Buffer.alloc(4);
                        tmpbuf.writeInt32LE(hash);
                        settingsHash = tmpbuf.toString("hex");

                        console.error(`got hash ${settingsHash}, retrying requests at ${new Date().toLocaleString("en-CA")}`);
                        continue;
                    }
                    case 2: {
                        console.error(`warning: unable to parse response, refetching server details at ${new Date().toLocaleString("en-CA")}`);
                        const server = await (await fetch(`http://circlebox.net/boom_arena/api/api_auto_update.php?${new URLSearchParams({
                            Mode: 0,
                            Os: 2,
                            DeviceId: DEVICE_ID,
                            Lang: "english",
                            ClientVersion: "",
                            Time: Date.now()
                        })}`)).json();

                        HOST = server.ServerAddress;
                        PORT = server.ServerPort;
                        failcount = 0;
                        continue;
                    }
                }
            }
            failcount = 0;
            const [replays, leaderboard] = data;

            let latest = await sqlPool.query(`SELECT id FROM games${CURRENT_SAMPLE_PERIOD.ladder} ORDER BY id DESC LIMIT 1`);
            let transaction;

            if (latest.rowCount === 0) {
                transaction = [];
                for (let i = 0; i < 50; i++)
                    transaction.push(...processLeaderboardData(leaderboard[i]));
                await processTransaction(transaction);

                for (let i = 0; i < replays.length; i++) {
                    transaction = await processReplayData(replays[i]);
                    await processTransaction(transaction);
                }
            } else {
                if (resetCondition(topmedals, leaderboard[0].medals))
                    break;
                topmedals = leaderboard[0].medals;

                latest = latest.rows[0].id;

                let rptr = replays.length;
                while (--rptr >= 0 && replays[rptr].id > latest);

                if (rptr === -1)
                    console.error(`warning: replay traffic too high at ${new Date().toLocaleString("en-CA")}`);

                for (let i = rptr + 1; i < replays.length; i++) {
                    transaction = await processReplayData(replays[i]);
                    await processTransaction(transaction);
                }

                transaction = [];
                for (let i = 0; i < 50; i++)
                    transaction.push(...processLeaderboardData(leaderboard[i]));
                await processTransaction(transaction);
            }
        } catch (err) {
            console.error(err, `at ${new Date().toLocaleString("en-CA")}`);
        }
        await new Promise(r => setTimeout(r, POLLINTERVAL_MS));
    }

    console.log(`reset happened at ${new Date().toLocaleString("en-CA")}`);
    while (true) { // eslint-disable-line
        await new Promise(r => setTimeout(r, 60000));
    }
}

async function initSql() {
    const gamecountids = (await sqlPool.query("SELECT id FROM gamecount")).rows.map(v => v.id);

    for (const period of SAMPLE_PERIODS.gamecount)
        if (!gamecountids.includes(period))
            await sqlPool.query({ text: "INSERT INTO gamecount VALUES ($1, 0)", values: [period] });

    const tables = (await sqlPool.query("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")).rows.map(v => v.tablename);

    for (const period of SAMPLE_PERIODS.players)
        if (!tables.includes(`players${period}`))
            await sqlPool.query(`CREATE TABLE players${period} (id int4 PRIMARY KEY, medals int4, wins int4, losses int4, draws int4, three_crown_wins int4, name varchar(30), games int4[], decks deckstats[])`);

    for (const period of SAMPLE_PERIODS.leaderboard) {
        if (!tables.includes(`leaderboard${period}`))
            await sqlPool.query(`CREATE TABLE leaderboard${period} (rank int4 PRIMARY KEY, user_id int4, medals int4, name varchar(30))`);

        for (let i = 0; i < 50; i++) {
            await sqlPool.query({ text: `INSERT INTO leaderboard${period} VALUES ($1, -1, 0, '') ON CONFLICT DO NOTHING`, values: [i] });
        }
    }

    for (const period of SAMPLE_PERIODS.games)
        if (!tables.includes(`games${period}`))
            await sqlPool.query(`CREATE TABLE games${period} (id int4 PRIMARY KEY, p0_id int4, p1_id int4, p0_medals int4, p1_medals int4, p0_towers int2, p1_towers int2, timestamp float8, p0_deck char(16), p1_deck char(16))`);

    for (const period of SAMPLE_PERIODS.decks)
        if (!tables.includes(`decks${period}`))
            await sqlPool.query(`CREATE TABLE decks${period} (id char(16) PRIMARY KEY, wins int4, losses int4, draws int4, rating float8, players int4[], card_matchups stats[])`);

    for (const period of SAMPLE_PERIODS.cards)
        if (!tables.includes(`cards${period}`))
            await sqlPool.query(`CREATE TABLE cards${period} (id int4 PRIMARY KEY, wins int4, losses int4, draws int4, rating float8, card_matchups stats[])`);

    for (const period of Object.values(SAMPLE_PERIODS.cards))
        for (const i in cards)
            await sqlPool.query({ text: `INSERT INTO cards${period} VALUES ($1, 0, 0, 0, 0, '{}'::stats[]) ON CONFLICT DO NOTHING`, values: [i] });
}

async function processTransaction(transaction) {
    const sqlClient = await sqlPool.connect();
    let i = 0;
    try {
        await sqlClient.query("BEGIN");
        for (; i < transaction.length; i++)
            await sqlClient.query(transaction[i]);
        await sqlClient.query("COMMIT");
    } catch (err) {
        await sqlClient.query("ROLLBACK");
        console.error(err, transaction[i], `at ${new Date().toLocaleString("en-CA")}`);
    }
    sqlClient.release();
}

async function processReplayData(replay) {
    const transaction = [];

    transaction.push({ text: "UPDATE gamecount SET count = count + 2 WHERE id = $1", values: [CURRENT_SAMPLE_PERIOD.ladder] });
    transaction.push({
        text: `INSERT INTO games${CURRENT_SAMPLE_PERIOD.ladder} VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        values: [replay.id, replay.players[0].id, replay.players[1].id, replay.players[0].medals, replay.players[1].medals, replay.players[0].towers, replay.players[1].towers, replay.timestamp, encodeDeck(replay.players[0].deck), encodeDeck(replay.players[1].deck)]
    });

    if (replay.players[0].towers > replay.players[1].towers) {
        const mutations = await processReplayDataPlayers(transaction, replay.players[0], replay.players[1], replay.id, "wins");
        await processReplayDataPlayers(transaction, replay.players[1], replay.players[0], replay.id, "losses", mutations);
    } else if (replay.players[0].towers < replay.players[1].towers) {
        const mutations = await processReplayDataPlayers(transaction, replay.players[0], replay.players[1], replay.id, "losses");
        await processReplayDataPlayers(transaction, replay.players[1], replay.players[0], replay.id, "wins", mutations);
    } else {
        const mutations = await processReplayDataPlayers(transaction, replay.players[0], replay.players[1], replay.id, "draws");
        await processReplayDataPlayers(transaction, replay.players[1], replay.players[0], replay.id, "draws", mutations);
    }

    return transaction;
}

async function processReplayDataPlayers(transaction, player, opponent, game, result, mutations = []) {
    const deck = encodeDeck(player.deck);

    let decks = (await sqlPool.query({ text: `SELECT decks FROM players${CURRENT_SAMPLE_PERIOD.ladder} WHERE id = $1`, values: [player.id] })).rows;
    if (decks.length === 0) {
        transaction.push({ text: `INSERT INTO players${CURRENT_SAMPLE_PERIOD.ladder} VALUES ($1, 0, 0, 0, 0, 0, '', '{}', '{}') ON CONFLICT DO NOTHING`, values: [player.id] });
        decks = [];
    } else {
        decks = parsePlayerDecks(decks[0].decks);
    }

    transaction.push({
        text: `UPDATE players${CURRENT_SAMPLE_PERIOD.ladder} SET name = $1, medals = $2, ${result} = ${result} + 1, games = $3::int4 || games WHERE id = $4`,
        values: [player.name || `USER-${player.id}`, player.medals, game, player.id]
    });

    if (opponent.towers === 0)
        transaction.push({ text: `UPDATE players${CURRENT_SAMPLE_PERIOD.ladder} SET three_crown_wins = three_crown_wins + 1 WHERE id = $1`, values: [player.id] });

    const deckmatch = decks.findIndex(v => v.id === deck);
    if (deckmatch === -1) {
        const playerDeckStats = {
            id: deck,
            wins: 0,
            losses: 0,
            draws: 0
        };
        playerDeckStats[result] = 1;
        transaction.push({
            text: `UPDATE players${CURRENT_SAMPLE_PERIOD.ladder} SET decks = ${toPlayerDeck(playerDeckStats)} || decks WHERE id = $1`,
            values: [player.id]
        });
    } else {
        transaction.push({
            text: `UPDATE players${CURRENT_SAMPLE_PERIOD.ladder} SET decks[$1].${result} = decks[$1].${result} + 1 WHERE id = $2`,
            values: [deckmatch + 1, player.id]
        });
        transaction.push({
            text: `UPDATE players${CURRENT_SAMPLE_PERIOD.ladder} SET decks = decks[$1] || array_remove(decks, decks[$1]) WHERE id = $2`,
            values: [deckmatch + 1, player.id]
        });
    }

    const isTopLadder = !!(await sqlPool.query({ text: `SELECT 1 FROM leaderboard${CURRENT_SAMPLE_PERIOD.ladder} WHERE user_id = $1`, values: [player.id] })).rowCount;
    const newMutations = [{}, {}];

    newMutations[0].decks = await processReplayDataDecks(CURRENT_SAMPLE_PERIOD.ladder, transaction, deck, opponent.deck, player, result, mutations[0]?.decks);
    newMutations[0].cards = await processReplayDataCards(CURRENT_SAMPLE_PERIOD.ladder, transaction, player.deck, opponent.deck, result, mutations[0]?.cards);

    if (isTopLadder) {
        transaction.push({ text: "UPDATE gamecount SET count = count + 1 WHERE id = $1", values: [CURRENT_SAMPLE_PERIOD.topladder] });
        newMutations[1].decks = await processReplayDataDecks(CURRENT_SAMPLE_PERIOD.topladder, transaction, deck, opponent.deck, player, result, mutations[1]?.decks);
        newMutations[1].decks = await processReplayDataCards(CURRENT_SAMPLE_PERIOD.topladder, transaction, player.deck, opponent.deck, result, mutations[1]?.cards);
    }

    return newMutations;
}

async function processReplayDataDecks(table, transaction, deck, opponentCards, player, result, mutations = {}) {
    let cardMatchups = (await sqlPool.query({ text: `SELECT card_matchups FROM decks${table} WHERE id = $1`, values: [deck] })).rows;
    if (cardMatchups.length === 0) {
        transaction.push({ text: `INSERT INTO decks${table} VALUES ($1, 0, 0, 0, 0, '{}', '{}') ON CONFLICT DO NOTHING`, values: [deck] });
        cardMatchups = [];
    } else {
        cardMatchups = [...parseCardMatchups(cardMatchups[0].card_matchups), ...(mutations[deck] ?? [])];
    }

    transaction.push({
        text: `UPDATE decks${table} SET ${result} = ${result} + 1, players = ARRAY(SELECT v FROM (SELECT v, MIN(o) FROM unnest($1::int4 || players) WITH ORDINALITY AS a(v, o) GROUP BY v ORDER BY MIN(o) ASC) AS a) WHERE id = $2`,
        values: [player.id, deck]
    });
    transaction.push({ text: `UPDATE decks${table} SET rating = calc_ba_rating(wins, losses, draws) WHERE id = $1`, values: [deck] });

    const newStats = [];
    for (let i = 0; i < 8; i++) {
        const cardmatch = cardMatchups?.findIndex(v => v.id === opponentCards[i]);
        if (cardmatch == null || cardmatch === -1) {
            const cardMatchupStats = {
                id: opponentCards[i],
                wins: 0,
                losses: 0,
                draws: 0
            };
            newStats.push(cardMatchupStats);
            cardMatchupStats[result] = 1;
            transaction.push({ text: `UPDATE decks${table} SET card_matchups = card_matchups || ${toCardMatchup(cardMatchupStats)} WHERE id = $1`, values: [deck] });
        } else {
            transaction.push({
                text: `UPDATE decks${table} SET card_matchups[$1].${result} = card_matchups[$1].${result} + 1 WHERE id = $2`,
                values: [cardmatch + 1, deck]
            });
        }
    }

    return { [deck]: newStats };
}

async function processReplayDataCards(table, transaction, deck, opponentCards, result, mutations = {}) {
    const newStats = {};
    for (let i = 0; i < 8; i++) {
        newStats[deck[i]] = [];

        transaction.push({
            text: `UPDATE cards${table} SET ${result} = ${result} + 1 WHERE id = $1`,
            values: [deck[i]]
        });
        transaction.push({ text: `UPDATE cards${table} SET rating = calc_ba_rating(wins, losses, draws) WHERE id = $1`, values: [deck[i]] });

        const cardMatchups = [...parseCardMatchups((await sqlPool.query({ text: `SELECT card_matchups FROM cards${table} WHERE id = $1`, values: [deck[i]] })).rows[0].card_matchups), ...(mutations[deck[i]] ?? [])];
        for (let j = 0; j < 8; j++) {
            const cardmatch = cardMatchups.findIndex(v => v.id === opponentCards[j]);
            if (cardmatch === -1) {
                const cardMatchupStats = {
                    id: opponentCards[j],
                    wins: 0,
                    losses: 0,
                    draws: 0
                };
                newStats[deck[i]].push(cardMatchupStats);
                cardMatchupStats[result] = 1;
                transaction.push({ text: `UPDATE cards${table} SET card_matchups = card_matchups || ${toCardMatchup(cardMatchupStats)} WHERE id = $1`, values: [deck[i]] });
            } else {
                transaction.push({
                    text: `UPDATE cards${table} SET card_matchups[$1].${result} = card_matchups[$1].${result} + 1 WHERE id = $2`,
                    values: [cardmatch + 1, deck[i]]
                });
            }
        }
    }
    return newStats;
}

function processLeaderboardData(entry) {
    const transaction = [];
    transaction.push({
        text: `UPDATE leaderboard${CURRENT_SAMPLE_PERIOD.ladder} SET user_id = $1, medals = $2, name = $3 WHERE rank = $4`,
        values: [entry.user, entry.medals, entry.name, entry.rank]
    });
    transaction.push({
        text: `INSERT INTO players${CURRENT_SAMPLE_PERIOD.ladder} VALUES ($1, $2, 0, 0, 0, 0, $3, '{}', '{}') ON CONFLICT (id) DO UPDATE SET medals = $2, name = $3`,
        values: [entry.user, entry.medals, entry.name]
    });
    return transaction;
}

function parsePlayerDecks(str) {
    const matches = str.match(/\([^)]*\)/g);
    return matches == null ? [] : matches.map(v => JSON.parse(`["${v.slice(1, -1).replace(/,/g, "\",\"")}"]`)).map(v => ({
        id: v[0],
        wins: parseInt(v[1]),
        losses: parseInt(v[2]),
        draws: parseInt(v[3])
    }));
}

function parseCardMatchups(str) {
    const matches = str.match(/\([^)]*\)/g);
    return matches == null ? [] : matches.map(v => JSON.parse(`["${v.slice(1, -1).replace(/,/g, "\",\"")}"]`)).map(v => ({
        id: parseInt(v[0]),
        wins: parseInt(v[1]),
        losses: parseInt(v[2]),
        draws: parseInt(v[3])
    }));
}

function toPlayerDeck(obj) {
    return `row('${obj.id}',${obj.wins},${obj.losses},${obj.draws})::deckstats`;
}

function toCardMatchup(obj) {
    return `row(${obj.id},${obj.wins},${obj.losses},${obj.draws})::stats`;
}

function encodeDeck(arr) {
    const a = [...arr];
    return a.sort((x, y) => x - y).map(v => v.toString(16).padStart(2, "0")).join("");
}

function parseResponse(buf) {
    const replays = [];
    const leaderboards = [];

    let i;

    for (i = 0; i < LEN_REPLAYS; i += 168) {
        if (buf.readInt32LE(i) !== 0xa8 || buf.readInt32LE(i + 4) !== 0xd4 || buf.readInt32LE(i + 8) !== 0x01)
            return null;

        replays.push(parseReplay(buf.subarray(i)));
    }

    for (; i < LEN_REPLAYS + LEN_LB_PLAYERS; i += 68) {
        if (buf.readInt32LE(i) !== 0x44 || buf.readInt32LE(i + 4) !== 0xd3 || buf.readInt32LE(i + 8) !== 0x01)
            return null;

        leaderboards.push(parseLeaderboard(buf.subarray(i)));
    }

    return [replays, leaderboards];
}

/*
    BOOM ARENA REPLAY (168)
    FORMAT: [content](bytes)
    LITTLE ENDIAN

    [a8 00 00 00 d4 00 00 00 01 00 00 00](12) // header     0
    [game id](4)                                            12
    [user id 1](4)                                          16
    [user id 2](4)                                          20
    [medals 1](4)                                           24
    [medals 2](4)                                           28
    [towers left 1](4)                                      32
    [towers left 2](4)                                      36
    [unknown dword](4)                                      40
    [deck 1](32) // card ids, 4 bytes each,                 44
    [deck 2](32) // in order of deck                        76
    [00 00 00 00](4)                                        108
    [unknown dword](4)                                      112
    [00 00 00 00](4)                                        116
    [name1](21)                                             120
    [name2](21)                                             141
    [00 00 00 00 00 00](6)                                  162
*/
function parseReplay(buf) {
    const p0deck = [], p1deck = [];

    for (let i = 0; i < 32; i += 4) {
        p0deck.push(buf[i + 44]);
        p1deck.push(buf[i + 76]);
    }

    let p0namestop = 119, p1namestop = 140;

    while (buf[++p0namestop] !== 0);
    while (buf[++p1namestop] !== 0);

    return {
        id: buf.readInt32LE(12),
        reserved: [buf.readInt32LE(40), buf.readInt32LE(112)],
        players: [
            {
                id: buf.readInt32LE(16),
                medals: buf.readInt32LE(24),
                towers: buf[32],
                deck: p0deck,
                name: buf.subarray(120, p0namestop).toString()
            },
            {
                id: buf.readInt32LE(20),
                medals: buf.readInt32LE(28),
                towers: buf[36],
                deck: p1deck,
                name: buf.subarray(141, p1namestop).toString()
            }
        ],
        timestamp: Date.now()
    };
}

/*
    BOOM ARENA LEADERBOARD (68)
    FORMAT: [content](bytes)
    LITTLE ENDIAN

    [44 00 00 00 d3 00 00 00 01 00 00 00](12) // header     0
    [ranking (0 indexed)](4)                                12
    [user id](4)                                            16
    [medals](4)                                             20
    [name](21)                                              24
    [clan name](16)                                         45
    [null padding](7)                                       61
*/
function parseLeaderboard(buf) {
    let namestop = 23;
    let clannamestop = 44;

    while (buf[++namestop] !== 0);
    while (buf[++clannamestop] !== 0);

    return {
        rank: buf.readInt32LE(12),
        user: buf.readInt32LE(16),
        medals: buf.readInt32LE(20),
        name: buf.subarray(24, namestop).toString(),
        clan: buf.subarray(45, clannamestop).toString()
    };
}

const FIRST_DATA_PACKET = 2;

function getResponse(port, host) {
    return new Promise((resolve, reject) => {
        const socket = new net.Socket();
        const response = [];
        let received = 0;
        let datalen = 0;

        socket.on("data", (data) => {
            if (data.readInt32LE(0) === 0x10) {
                if (data.length <= 16)
                    return;
                data = data.subarray(16);
            }

            if (++received === 1)
                socket.write(Buffer.from(REQUESTS.replays_leaderboard, "hex"));

            if (received >= FIRST_DATA_PACKET) {
                response.push(data);
                datalen += data.length;
                if (datalen >= LEN_REPLAYS + LEN_LB_PLAYERS) {
                    socket.destroy();
                    resolve(Buffer.concat(response));
                }
            }
        });

        socket.on("error", err => console.error(`socket error: ${err}`));
        socket.on("close", () => reject("socket close"));
        socket.on("timeout", socket.destroy);
        socket.connect(port, host, () => socket.write(Buffer.from(REQUESTS.login, "hex")));
        setTimeout(() => {
            if (!socket.destroyed) {
                console.error(`warning: getResponse timed out with ${Buffer.concat(response).length} bytes received at ${new Date().toLocaleString("en-CA")}`);
                socket.destroy();
            }
        }, TIMEOUT_MS);
    });
}

function settingsHashCode(segments) {
    let hash = 0;
    let i = 0;
    do {
        const segment = segments[i];
        i++;
        for (let j = 0; j < segment.length; j++)
            hash += i * segment.charCodeAt(j);
        hash++;
    } while (i < segments.length);
    return hash;
}

function parseSettings(buf) {
    const settings = {};
    let temp = [];
    for (let i = 0; i < buf.length - 8; i++) {
        if (buf.readInt32LE(i) !== 0x71)
            continue;
        switch (buf.readInt32LE(i + 4)) {
            case 0x01:
                i += 12;
                break;
            case 0x02: {
                const s = i + 8;
                i += 7;
                while (buf[++i] !== 0);
                const data = buf.subarray(s, i);
                temp.push(data);
                i += 4;
                break;
            }
            case 0x03: {
                const s = i + 8;
                i += 7;
                while (buf[++i] !== 0);
                settings[buf.subarray(s, i).toString()] = Buffer.concat(temp).toString().split("|");
                temp = [];
                i += 4;
                break;
            }
            case 0x04:
                return settings;
        }
    }
    return settings;
}

function getSettings(port, host) {
    return new Promise((resolve, reject) => {
        const socket = new net.Socket();
        let response = Buffer.alloc(0);

        socket.on("data", (data) => {
            if (data.readInt32LE(0) === 0x10) {
                if (data.length <= 16)
                    return;
                data = data.subarray(16);
            }

            response = Buffer.concat([response, data]);
            if (response.length >= 11 &&
                (response.readInt32LE(response.length - 8) === 0x71 && response.readInt32LE(response.length - 4) === 0x04) ||
                (response.readInt32LE(response.length - 9) === 0x71 && response.readInt32LE(response.length - 5) === 0x04) ||
                (response.readInt32LE(response.length - 10) === 0x71 && response.readInt32LE(response.length - 6) === 0x04) ||
                (response.readInt32LE(response.length - 11) === 0x71 && response.readInt32LE(response.length - 7) === 0x04)) { // too lazy to figure out where it actually is so i just check everything
                socket.destroy();
                resolve(response);
            }
        });

        socket.on("error", err => console.error(`socket error: ${err}`));
        socket.on("close", () => reject("socket close"));
        socket.on("timeout", socket.destroy);
        socket.connect(port, host, () => socket.write(Buffer.from(REQUESTS.login, "hex")));

        setTimeout(() => {
            if (!socket.destroyed) {
                console.error(`warning: getSettings timed out at ${new Date().toLocaleString("en-CA")}`);
                socket.destroy();
            }
        }, TIMEOUT_MS);
    });
}
