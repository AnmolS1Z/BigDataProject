const express = require('express');
const app = express();
const hbase = require('hbase');
const filesystem = require('fs');
const mustache = require('mustache');
const { URL } = require('url');

const LOWER48_STATES = new Set([
    'AL','AZ','AR','CA','CO','CT','DE','FL','GA','ID','IL','IN','IA','KS','KY','LA',
    'ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND',
    'OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
]);

const port = Number(process.argv[2]);
const url = new URL(process.argv[3]);

const hclient = hbase({
    host: url.hostname,
    path: url.pathname ?? "/",
    port: url.port,
    protocol: url.protocol.slice(0, -1),
    encoding: 'latin1',
    auth: process.env.HBASE_AUTH
});

function buildRowKey(state, year, month) {
    const m = month.toString().padStart(2, '0');
    return `${state.toUpperCase()}#${year}#${m}`;
}

function rowToMap(row) {
    const stats = {};
    row.forEach(item => {
        stats[item.column] = item.$;
    });
    return stats;
}

function longFromBinary(value) {
    if (value == null) return 0;
    const buf = Buffer.from(value, 'binary');  // 8 bytes
    return Number(buf.readBigInt64BE());
}

app.get('/', (req, res) => {
    const template = filesystem.readFileSync("result.mustache").toString();

    const html = mustache.render(template, {
        state: '',
        year: '',
        month: '',
        // nothing searched yet:
        show_result: false,
        show_error: false
    });

    res.send(html);
});

app.get('/search', (req, res) => {
    const template = filesystem.readFileSync("result.mustache").toString();

    const stateRaw = req.query.state || '';
    const yearRaw  = req.query.year || '';
    const monthRaw = req.query.month || '';

    const state = stateRaw.toUpperCase().trim();
    const year  = yearRaw.trim();
    const month = monthRaw.trim();

    if (!state || !year || !month) {
        const html = mustache.render(template, {
            state: '',
            year: '',
            month: '',
            show_result: false,
            show_error: false
        });
        return res.send(html);
    }

    if (!LOWER48_STATES.has(state)) {
        const html = mustache.render(template, {
            state,
            year,
            month,
            show_result: false,
            show_error: true,
            show_missing: false,
            error_message:
                `${state} is not a valid 2-letter state code in the contiguous United States. ` +
                `Please enter a lower 48 state such as CA, NV, or WA.`
        });
        return res.status(400).send(html);
    }

    const rowKey = buildRowKey(state, year, month);
    console.log("Looking up row:", rowKey);

    hclient.table('anmolsandhu_quake_state_month_hb')
        .row(rowKey)
        .get((err, cells) => {
            const template = filesystem.readFileSync("result.mustache").toString();

            // --- Case 1: HBase error (connection, server down, etc.) ---
            if (err) {
                console.error("HBase error:", err);
                const errStr = String(err || '');

                // Many HBase REST clients use 404 / "Not found" for missing rows.
                const looksLikeMissing =
                    errStr.includes('404') ||
                    errStr.toLowerCase().includes('not found');

                if (looksLikeMissing) {
                    const html = mustache.render(template, {
                        state,
                        year,
                        month,
                        show_result: false,
                        show_error: false,
                        show_missing: true,
                        missing_message:
                            `The state ${state} was not included in the historical ` +
                            `dataset (~3 MB) used for this project, so predictions ` +
                            `weren't generated. With a larger dataset download, this ` +
                            `state could be supported, though it is unlikely to have a >` +
                            `4.0 magnitude earthquake in any case.`
                    });
                    return res.status(200).send(html);
                }

                // Real internal error
                const html = mustache.render(template, {
                    state,
                    year,
                    month,
                    show_result: false,
                    show_error: true,
                    show_missing: false,
                    error_message: "Internal HBase error. Please try again."
                });
                return res.status(500).send(html);
            }

            // --- Case 2: no cells returned (row truly missing) ---
            if (!cells || cells.length === 0) {
                const html = mustache.render(template, {
                    state,
                    year,
                    month,
                    show_result: false,
                    show_error: false,
                    show_missing: true,
                    missing_message:
                        `The state ${state} was not included in the historical ` +
                        `dataset (~3 MB) used for this project, so predictions ` +
                        `weren't generated. With a larger dataset download, this ` +
                        `state could be supported.`
                });
                return res.status(200).send(html);
            }

            // --- Case 3: normal success path ---
            const stats = rowToMap(cells);

            const quakeCount = longFromBinary(stats['s:quake_count']);
            const maxMag     = Number(stats['s:max_mag'] || 0).toFixed(2);
            const label      = Number(stats['s:label_quake_ge_4'] || 0);
            const prob       = Number(stats['s:pred_prob_ge_4'] || 0);

            const html = mustache.render(template, {
                state,
                year,
                month,
                quake_count: quakeCount,
                max_mag: maxMag,
                quake_occurred: label === 1 ? "Yes" : "No",
                pred_prob_percent: (prob * 100).toFixed(2) + " %",
                show_result: true,
                show_error: false,
                show_missing: false
            });

            res.send(html);
        });

});
app.use(express.static('public'));

app.listen(port);
