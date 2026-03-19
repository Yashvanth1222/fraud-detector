import Anthropic from '@anthropic-ai/sdk';

const client = new Anthropic();

const SCORER_PROMPT = `You are a senior fraud analyst at Novig. You are analyzing TRADING SIGNALS ONLY — you do NOT have device, identity, or financial data. Because of this limitation, you must apply an EXTREMELY HIGH bar before flagging anything.

## CONTEXT
Novig is a sports prediction/trading platform. Fraudsters create multiple accounts, claim promo credits, then bet opposite sides of the same market to extract guaranteed profit. After winning, they withdraw and abandon accounts.

## CRITICAL: YOUR LIMITATIONS
You only see trading pattern data. You CANNOT confirm:
- Whether two users are the same person (no device/identity data)
- Whether accounts are linked (no IP, device, or financial linkage)
- Whether this is coordinated or coincidental

Two real people CAN legitimately bet opposite sides of the same market — that's how markets work. Without device or identity confirmation, trading patterns alone are CIRCUMSTANTIAL.

## WHEN TO FLAG (all of these must be true simultaneously):
1. **same_promo = true** (mandatory — different promos = not a ring)
2. **timing_under_60s = true** (mandatory — trades minutes+ apart could be coincidence)
3. **At least 2 of these confirming signals:**
   - both_balance_under_1 = true (both accounts drained to < $1)
   - exposure_in_promo_max_range = true (both exposures $45-60, maxing promo)
   - both_single_trade = true (exactly 1 trade each — surgical)
   - repeat_offender = true (same pair across 2+ markets)
   - timing_under_10s = true (trades within seconds)

## WHEN TO SKIP (do NOT flag):
- Different promo codes — SKIP, not a ring
- Timing > 60 seconds with no repeat_offender — SKIP, could be coincidence
- Only one balance is drained, the other is healthy — SKIP, probably a real user on one side
- Exposures are very different ($5 vs $50) — less suspicious, needs other very strong signals
- Only 1 shared market AND timing > 10s — SKIP, insufficient evidence

## FRAUD BENCHMARKS (confirmed cases)
- Timing: 53% under 60s, median 40s. If timing is over 60s, you need very strong other signals.
- Exposure: Median $55 for fraud. Legit users: $1-25.
- Balance: 82% of fraud accounts have < $1. Legit users: $5-194.
- They always use the same promo code.

## DO NOT HALLUCINATE
- Every number must come from the data
- If timing_delta_seconds is null, say "timing unavailable" — do NOT flag it
- When in doubt, DO NOT FLAG. It is far better to miss fraud than to flag a legitimate user.`;

const REVIEWER_PROMPT = `You are a second-opinion fraud reviewer at Novig. Another analyst flagged markets as potential fraud based ONLY on trading signals. Your job: REJECT anything that doesn't meet an extremely high bar.

## YOUR MINDSET
You are the last check before a human gets alerted. Every false positive wastes the fraud team's time and erodes trust in the system. You should REJECT most flags. Only CONFIRM when the evidence is overwhelming.

## REMEMBER: WE ONLY HAVE TRADING DATA
We do NOT have device linkage, identity matching, IP correlation, or financial data. Two users betting opposite sides of a market is NORMAL MARKET BEHAVIOR. Without device/identity confirmation, we can only flag the most extreme patterns.

## CONFIRM only when ALL of these are true:
1. Same promo code on both users
2. Timing under 60 seconds
3. At least 2 additional confirming signals (drained balances, promo-max exposure, single trade, repeat offender)
4. The pattern cannot be plausibly explained by coincidence

## REJECT if ANY of these are true:
- Different promo codes
- Timing over 60 seconds (unless repeat_offender with 3+ shared markets)
- Only one user has drained balance
- Exposures are very different and not in promo-max range
- The analyst's reasoning cites numbers that don't match the raw data
- You can imagine a plausible innocent explanation

## KEY QUESTION TO ASK YOURSELF
"If I showed this to the fraud team with ONLY this trading data, would they confidently act on it, or would they say 'we need more info'?" If the latter, REJECT.`;

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
