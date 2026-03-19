import Anthropic from '@anthropic-ai/sdk';

const client = new Anthropic();

const SCORER_PROMPT = `You are a senior fraud analyst at Novig. You are analyzing trading signals including DIRECT COUNTERPARTY data — meaning you can see when two users literally traded against each other, not just that they were in the same market.

## CONTEXT
Novig is a sports prediction/trading platform. Fraudsters create multiple accounts, claim promo credits, then bet opposite sides of the same market to extract guaranteed profit. After winning, they withdraw and abandon accounts.

## THE KEY SIGNAL: DIRECT COUNTERPARTY
The data now includes whether two users DIRECTLY traded against each other (their orders were matched). This is much stronger than just "opposite sides of the same market":
- **direct_counterparty = true**: their money literally went against each other. Combined with same promo + other signals, this is very strong evidence.
- **direct_counterparty = false or missing**: they were in the same market but may have traded against market makers or other users, not each other. This is weaker.

Counterparty types: "User" = another real user. "EMM" or "IMM_LMSR" = market maker (normal, not suspicious).

## TWO PATHS TO FLAG:

**Path 1 — Direct counterparty (high confidence):**
All must be true:
1. direct_counterparty = true (they literally traded against each other)
2. same_promo = true
3. At least 1 confirming signal: both_balance_under_1, timing_under_60s, exposure match, both_single_trade, or repeat_offender

**Path 2 — No direct counterparty (need more evidence):**
All must be true:
1. same_promo = true
2. timing_under_60s = true
3. At least 2 confirming signals: both_balance_under_1, exposure_in_promo_max_range, both_single_trade, repeat_offender, timing_under_10s

## WHEN TO SKIP (do NOT flag):
- Different promo codes — SKIP
- No direct counterparty AND timing > 60s AND not a repeat offender — SKIP
- Only one balance is drained, the other is healthy — SKIP
- Counterparty types are only EMM/IMM_LMSR (market makers) — SKIP, that's normal trading

## FRAUD BENCHMARKS
- Timing: 53% under 60s, median 40s
- Exposure: Median $55 for fraud, legit $1-25
- Balance: 82% of fraud < $1, legit $5-194
- They always use the same promo code
- Direct counterparty between two promo users on opposite sides = very strong signal

## DO NOT HALLUCINATE
- Every number must come from the data
- If timing_delta_seconds is null, say "timing unavailable"
- When in doubt, DO NOT FLAG. Better to miss fraud than flag a legitimate user.`;

const REVIEWER_PROMPT = `You are a second-opinion fraud reviewer at Novig. Another analyst flagged markets as potential fraud. Your job: REJECT anything that doesn't meet an extremely high bar.

## YOUR MINDSET
You are the last check before a human gets alerted. Every false positive wastes the fraud team's time. REJECT most flags. Only CONFIRM when evidence is overwhelming.

## WHAT DATA WE HAVE
We have trading patterns AND direct counterparty data (whether two users literally traded against each other). We do NOT have device, identity, IP, or financial data.

## CONFIRM when:
**If direct_counterparty = true:**
- Same promo + direct counterparty + 1 confirming signal (drained balances, exposure match, single trade, tight timing) = CONFIRM

**If no direct counterparty:**
- Same promo + timing under 60s + 2 confirming signals + no plausible innocent explanation = CONFIRM

## REJECT if ANY of these are true:
- Different promo codes
- No direct counterparty AND timing over 60s (unless repeat_offender with 3+ shared markets)
- Only one user has drained balance, the other is healthy
- Counterparty types are market makers (EMM/IMM_LMSR), not users
- The analyst cited numbers that don't match the raw data
- You can imagine a plausible innocent explanation

## KEY QUESTION
"Would the fraud team confidently act on this?" If they'd say "we need more info," REJECT.`;

function parseJSON(text) {
  let clean = text.trim();
  clean = clean.replace(/^```json\s*/, '').replace(/\s*```$/, '').trim();
  return JSON.parse(clean);
}

function trimMarkets(markets) {
  return markets.map(m => {
    if (m.pairs.length <= 30) return m;
    const sorted = [...m.pairs].sort((a, b) => {
      const scoreA = (a.signals.timing_under_10s ? 3 : a.signals.timing_under_60s ? 2 : a.signals.timing_under_5min ? 1 : 0)
        + (a.signals.same_promo ? 2 : 0) + (a.signals.both_balance_under_1 ? 1 : 0) + (a.signals.exposure_within_5 ? 1 : 0);
      const scoreB = (b.signals.timing_under_10s ? 3 : b.signals.timing_under_60s ? 2 : b.signals.timing_under_5min ? 1 : 0)
        + (b.signals.same_promo ? 2 : 0) + (b.signals.both_balance_under_1 ? 1 : 0) + (b.signals.exposure_within_5 ? 1 : 0);
      return scoreB - scoreA;
    });
    return { ...m, pairs: sorted.slice(0, 30), total_pairs_truncated: m.pairs.length };
  });
}

export async function scoreMarkets(markets) {
  const trimmed = trimMarkets(markets);
  const dataStr = JSON.stringify(trimmed);
  const msg = await client.messages.create({
    model: 'claude-sonnet-4-20250514',
    max_tokens: 8192,
    temperature: 0.1,
    messages: [{
      role: 'user',
      content: `${SCORER_PROMPT}

## DATA (${markets.length} markets analyzed)
\`\`\`json
${dataStr}
\`\`\`

## YOUR TASK
Analyze each market. For markets containing definite fraud, report the fraud ring.

Respond with ONLY valid JSON, no markdown fences:
{
  "fraud_markets": [
    {
      "market_description": "exact market description from data",
      "market_id": "exact market_id from data",
      "user_ids": ["uuid1", "uuid2"],
      "evidence": {
        "timing_deltas": ["6s between user1 and user2"],
        "exposures": ["user1: $50.00", "user2: $52.50"],
        "promo_codes": ["5FOR50"],
        "balances": ["user1: $0.004", "user2: $0.008"],
        "users_in_market": 2,
        "shared_markets": 1
      },
      "reasoning": "2-3 sentences explaining WHY this is definite fraud, citing specific numbers"
    }
  ],
  "clean_markets": ["market descriptions that were analyzed but found clean"],
  "summary": "Found X fraud markets involving Y accounts. Z markets were clean."
}`
    }]
  });

  const text = msg.content[0]?.text || '{}';
  try {
    return parseJSON(text);
  } catch {
    console.error('Scorer parse failed:', text.slice(0, 200));
    return { fraud_markets: [], clean_markets: [], summary: 'Scorer parsing failed.' };
  }
}

export async function reviewMarkets(scorerResult, rawMarkets) {
  const msg = await client.messages.create({
    model: 'claude-sonnet-4-20250514',
    max_tokens: 8192,
    temperature: 0.1,
    messages: [{
      role: 'user',
      content: `${REVIEWER_PROMPT}

## WHAT THE FIRST ANALYST FLAGGED
\`\`\`json
${JSON.stringify(scorerResult.fraud_markets)}
\`\`\`

Summary: ${scorerResult.summary}

## RAW PRE-COMPUTED DATA (same data the first analyst saw)
\`\`\`json
${JSON.stringify(rawMarkets)}
\`\`\`

## YOUR TASK
For each flagged market:
1. **Verify the numbers** — do timing, exposures, balances, user_ids match the raw data? If the analyst cited wrong numbers, REJECT.
2. **Check the logic** — does the combination of signals prove fraud beyond reasonable doubt?
   - Timing > 5min with no other strong signals = NOT fraud
   - Different promos + healthy balances = NOT fraud
   - Could these just be two normal users who happened to bet opposite sides? If yes, REJECT.
3. **Decision**: CONFIRM or REJECT each market.

Respond with ONLY valid JSON, no markdown fences:
{
  "confirmed": [
    {
      "market_description": "exact market description",
      "market_id": "exact market_id",
      "user_ids": ["uuid1", "uuid2"],
      "reasoning": "2-3 sentences: your independent assessment citing specific numbers from the raw data"
    }
  ],
  "rejected": [
    {
      "market_description": "market description",
      "user_ids": ["uuid1", "uuid2"],
      "reason": "Why this isn't definite fraud"
    }
  ],
  "summary": "Confirmed X of Y flagged markets. Rejected Z."
}`
    }]
  });

  const text = msg.content[0]?.text || '{}';
  try {
    return parseJSON(text);
  } catch {
    console.error('Reviewer parse failed:', text.slice(0, 200));
    return { confirmed: [], rejected: [], summary: 'Reviewer parsing failed.' };
  }
}
