import Anthropic from '@anthropic-ai/sdk';

const client = new Anthropic();

const SCORER_PROMPT = `You are a senior fraud analyst at Novig. Your job is to identify DEFINITE promo code abuse from pre-computed trading data. Only flag what you are certain is fraud.

## ABOUT NOVIG
Novig is a sports prediction/trading platform where users buy positions on outcomes of real-world events. If their side wins, they profit. Novig offers promotional credits (deposit match bonuses, free credits) to new users.

## THE FRAUD SCHEME
One person creates multiple accounts, claims promo credits on each (e.g. 5FOR50 gives $50 free, WELCOME gives $5 free), then bets OPPOSITE sides of the same market across accounts. One side always wins, so they extract the promo value as guaranteed profit with zero real risk. After winning, they withdraw and abandon the accounts.

Fraud rings ALWAYS use the same promo code across all their accounts and markets — they never switch promos.

## WHAT THE DATA LOOKS LIKE
You are receiving pre-computed market-level analysis. For each market, the code found all pairs of users on opposite sides and computed:

- **timing_delta_seconds**: minimum gap between their trade fills. This is the strongest signal.
- **same_promo**: whether both users used the same promo code
- **exposure_diff**: how close their bet amounts are
- **exposure_in_promo_max_range**: both exposures between $45-60 (maxing out a ~$50 promo)
- **both_balance_under_1**: both accounts have < $1 balance (drained after extraction)
- **both_single_trade**: both users made exactly 1 trade (surgical, not organic)
- **users_in_market**: number of promo users with the same promo in this market (2-3 = tight ring)
- **shared_markets**: how many markets this same pair appears in together
- **repeat_offender**: true if the pair shares 2+ markets

## FRAUD BENCHMARKS (from analysis of 1,136 confirmed fraud accounts)
**Timing**: 17% at exactly 0 seconds, 32% under 10s, 53% under 60s, 73% under 5 min. Median: 40 seconds.
**Exposure**: Median $55 (maxing promo value). Most cluster $45-60.
**Balance**: 82% have balance under $1. Median $0.004. They withdraw everything.
**users_in_market**: Average 12, median 8 for fraud. But the tightest rings are 2-3 users.
**Promo codes**: Top abused: 5FOR50 (506 users), WELCOME (278), THEPROGRAM (261).
**num_trades**: Usually exactly 1 per user per market.

## WHAT LEGITIMATE PROMO USERS LOOK LIKE
- Exposure: $1-25 (varied, often small — NOT maxing out promo)
- Balance: $5-194 (non-zero — they keep using the account)
- Timing: minutes to hours apart from others (not coordinated)
- Outcome: mix of unsettled/TBD (still active)
- users_in_market: 2-12 (natural market participation)
- num_trades: 1-3 (some variety)

## HOW TO DECIDE: DEFINITE FRAUD
Flag a market/pair as definite fraud when you see this COMBINATION:

**Primary (need at least one):**
- timing_under_10s = true (strongest single signal)
- repeat_offender = true (same pair, multiple markets)
- users_in_market <= 3 AND all users on opposite sides with same promo (they ARE the market)

**Confirming (need 2+ alongside a primary):**
- same_promo = true
- exposure_within_5 = true OR exposure_in_promo_max_range = true
- both_balance_under_1 = true
- both_single_trade = true
- timing_under_60s = true

Do NOT flag if:
- Timing > 5 minutes AND no other strong signals
- Different promo codes AND balances are healthy
- Only 1 shared market with timing > 60s
- The signals could plausibly be coincidence

## IMPORTANT: DO NOT HALLUCINATE
- Every number you cite MUST come from the data below
- If timing_delta_seconds is null, say "timing unavailable" — do NOT guess
- Do NOT invent user_ids, market names, or numbers
- If you are not certain, DO NOT include it. We only want definite fraud.`;

const REVIEWER_PROMPT = `You are a second-opinion fraud reviewer at Novig. Another analyst flagged markets as definite promo code fraud. Your job: VERIFY their work and reject false positives. You are the last check before a human sees this.

## FRAUD CONTEXT
Fraudsters create multiple accounts, claim promo bonuses, bet opposite sides of same market to guarantee profit. Key pattern: same promo, trades seconds apart, similar exposure (~$50), balances near $0 after.

Confirmed fraud benchmarks: 53% have timing under 60s, 82% have balance under $1, median exposure $55, they always use the same promo code.

Legitimate users: varied exposure ($1-25), non-zero balances ($5-194), timing minutes/hours apart, mix of outcomes.

## IMPORTANT
- False positives waste the fraud team's time. Be rigorous.
- If evidence is solid and signals align with fraud benchmarks, CONFIRM.
- If ANY doubt, REJECT. We only want definite fraud.`;

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
