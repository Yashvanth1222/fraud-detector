import 'dotenv/config';
import cron from 'node-cron';
import { readSheet, appendRows } from './sheets.js';
import { clusterRows, parseRow } from './cluster.js';
import { scoreMarkets, reviewMarkets } from './analyze.js';
import { sendToN8N } from './notify.js';

const FRAUD_SHEET_ID = process.env.FRAUD_SHEET_ID;
const ANALYZED_SHEET_ID = process.env.ANALYZED_SHEET_ID;

async function run() {
  const startTime = Date.now();
  console.log(`[${new Date().toISOString()}] Starting fraud detection run...`);

  // Step 1: Read already-analyzed keys
  let analyzedKeys = new Set();
  try {
    const analyzedRows = await readSheet(ANALYZED_SHEET_ID);
    for (const r of analyzedRows) {
      if (r.user_id && r.market_id) {
        analyzedKeys.add(`${r.user_id}|${r.market_id}`);
      }
    }
    console.log(`Loaded ${analyzedKeys.size} already-analyzed keys`);
  } catch (e) {
    console.log('No existing analyzed data (fresh start)');
  }

  // Step 2: Read fraud sheet
  const rawRows = await readSheet(FRAUD_SHEET_ID);
  console.log(`Read ${rawRows.length} rows from fraud sheet`);

  // Step 3: Filter — remove locked, already-analyzed
  const filtered = rawRows.filter(r => {
    if (!r.user_id || !r.market_id) return false;
    if (String(r.is_locked || '').toLowerCase() === 'locked') return false;
    if (analyzedKeys.has(`${r.user_id}|${r.market_id}`)) return false;
    return true;
  });
  console.log(`${filtered.length} new rows to analyze (after filtering locked + already-analyzed)`);

  if (filtered.length === 0) {
    console.log('Nothing new to analyze. Done.');
    return;
  }

  // Step 4: Parse rows
  const rows = filtered.map(parseRow);

  // Step 5: Cluster by market
  const markets = clusterRows(rows);
  console.log(`Found ${markets.length} markets with suspicious opposite-side pairs`);

  // Step 6: Write ALL analyzed rows to sheet (before AI, so we don't re-process on failure)
  const now = new Date().toISOString().replace('T', ' ').substring(0, 19);
  const seen = new Set();
  const analyzedOutput = [];
  for (const r of rows) {
    const key = `${r.user_id}|${r.market_id}`;
    if (seen.has(key)) continue;
    seen.add(key);
    analyzedOutput.push({ user_id: r.user_id, market_id: r.market_id, analyzed_at: now, fraud_score: 0 });
  }
  await appendRows(ANALYZED_SHEET_ID, analyzedOutput);
  console.log(`Wrote ${analyzedOutput.length} rows to AlreadyAnalyzed sheet`);

  if (markets.length === 0) {
    console.log('No suspicious markets found. Done.');
    return;
  }

  // Step 7: AI Scorer (process in batches, sized to fit context window)
  // Rough estimate: 1 token ≈ 4 chars. Keep under 150k tokens for safety.
  const MAX_CHARS = 500000;
  let allConfirmed = [];

  const batches = [];
  let currentBatch = [];
  let currentChars = 0;
  for (const m of markets) {
    const mChars = JSON.stringify(m).length;
    if (currentBatch.length > 0 && (currentChars + mChars > MAX_CHARS || currentBatch.length >= 10)) {
      batches.push(currentBatch);
      currentBatch = [];
      currentChars = 0;
    }
    currentBatch.push(m);
    currentChars += mChars;
  }
  if (currentBatch.length > 0) batches.push(currentBatch);

  console.log(`Processing ${markets.length} markets in ${batches.length} batches`);

  for (let bi = 0; bi < batches.length; bi++) {
    const batch = batches[bi];
    console.log(`Scoring batch ${bi + 1}/${batches.length} (${batch.length} markets)...`);

    try {
      const scorerResult = await scoreMarkets(batch);
      const fraudCount = (scorerResult.fraud_markets || []).length;
      console.log(`Scorer found ${fraudCount} fraud markets in this batch`);

      if (fraudCount === 0) continue;

      // Step 8: AI Reviewer
      console.log(`Reviewing ${fraudCount} flagged markets...`);
      const reviewResult = await reviewMarkets(scorerResult, batch);
      const confirmed = reviewResult.confirmed || [];
      console.log(`Reviewer confirmed ${confirmed.length}, rejected ${(reviewResult.rejected || []).length}`);

      allConfirmed.push(...confirmed);
    } catch (e) {
      console.error(`Batch ${bi + 1} failed: ${e.message}. Skipping.`);
    }
  }

  // Step 9: Send confirmed fraud to n8n
  if (allConfirmed.length > 0) {
    console.log(`Sending ${allConfirmed.length} confirmed fraud markets to n8n...`);
    await sendToN8N(allConfirmed, rows);

    // Update analyzed sheet with fraud scores for confirmed accounts
    const fraudUpdates = [];
    for (const market of allConfirmed) {
      for (const uid of (market.user_ids || [])) {
        fraudUpdates.push({
          user_id: uid,
          market_id: market.market_id,
          analyzed_at: now,
          fraud_score: 3
        });
      }
    }
    if (fraudUpdates.length > 0) {
      await appendRows(ANALYZED_SHEET_ID, fraudUpdates);
    }
  } else {
    console.log('No confirmed fraud this run.');
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`Run complete in ${elapsed}s. ${allConfirmed.length} fraud markets confirmed.`);
}

// Run once if --once flag
if (process.argv.includes('--once')) {
  run().catch(e => { console.error('Run failed:', e); process.exit(1); });
} else {
  // Run on startup
  run().catch(e => console.error('Initial run failed:', e));

  // Then every 30 minutes
  cron.schedule('*/30 * * * *', () => {
    run().catch(e => console.error('Scheduled run failed:', e));
  });

  console.log('Fraud detector running. Cron: every 30 minutes.');
}
