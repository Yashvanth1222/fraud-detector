function parseTimestamps(str) {
  if (!str) return [];
  return String(str).split(',').map(s => s.trim()).filter(Boolean).map(s => {
    const d = new Date(s.includes('UTC') || s.includes('+') || s.includes('-') ? s : s + ' UTC');
    return isNaN(d.getTime()) ? null : d;
  }).filter(Boolean);
}

function minTimingDelta(ts1, ts2) {
  let min = Infinity;
  for (const a of ts1) {
    for (const b of ts2) {
      const delta = Math.abs(a.getTime() - b.getTime()) / 1000;
      if (delta < min) min = delta;
    }
  }
  return min === Infinity ? null : min;
}

export function clusterRows(rows) {
  // Group by market_id
  const byMarket = {};
  for (const r of rows) {
    if (!byMarket[r.market_id]) byMarket[r.market_id] = [];
    byMarket[r.market_id].push(r);
  }

  const markets = [];

  for (const [marketId, marketRows] of Object.entries(byMarket)) {
    if (marketRows.length < 2) continue;

    const sides = {};
    for (const r of marketRows) {
      const side = String(r.outcome_side || '').toLowerCase();
      if (!sides[side]) sides[side] = [];
      sides[side].push(r);
    }

    const sideKeys = Object.keys(sides);
    if (sideKeys.length < 2) continue;

    const pairs = [];
    for (let si = 0; si < sideKeys.length; si++) {
      for (let sj = si + 1; sj < sideKeys.length; sj++) {
        for (const u1 of sides[sideKeys[si]]) {
          for (const u2 of sides[sideKeys[sj]]) {
            if (u1.user_id === u2.user_id) continue;

            const ts1 = parseTimestamps(u1.placed_at_list);
            const ts2 = parseTimestamps(u2.placed_at_list);
            const timingDelta = minTimingDelta(ts1, ts2);
            const exposureDiff = Math.abs(u1.user_market_exposure - u2.user_market_exposure);

            // Check if these two users directly traded against each other
            const u1Counterparties = String(u1.counterparty_user_ids || '').split(',').map(s => s.trim()).filter(Boolean);
            const u2Counterparties = String(u2.counterparty_user_ids || '').split(',').map(s => s.trim()).filter(Boolean);
            const isDirectCounterparty = u1Counterparties.includes(u2.user_id) || u2Counterparties.includes(u1.user_id);

            // Check counterparty types — filter to only "User" type (not EMM/IMM_LMSR market makers)
            const u1Types = String(u1.counterparty_types || '').split(',').map(s => s.trim());
            const u2Types = String(u2.counterparty_types || '').split(',').map(s => s.trim());
            const hasUserCounterparty = u1Types.includes('User') || u2Types.includes('User');

            pairs.push({
              user_a: {
                user_id: u1.user_id,
                promo_code: u1.promo_code,
                side: u1.outcome_side,
                exposure: u1.user_market_exposure,
                balance: u1.balance,
                num_trades: u1.num_trades,
                timestamps: String(u1.placed_at_list || ''),
                outcome: u1.outcome_statuses,
                pnl: u1.total_pnl,
                is_direct_counterparty_in_sheet: u1.is_direct_counterparty_in_sheet
              },
              user_b: {
                user_id: u2.user_id,
                promo_code: u2.promo_code,
                side: u2.outcome_side,
                exposure: u2.user_market_exposure,
                balance: u2.balance,
                num_trades: u2.num_trades,
                timestamps: String(u2.placed_at_list || ''),
                outcome: u2.outcome_statuses,
                pnl: u2.total_pnl,
                is_direct_counterparty_in_sheet: u2.is_direct_counterparty_in_sheet
              },
              signals: {
                timing_delta_seconds: timingDelta !== null ? Math.round(timingDelta) : null,
                timing_under_10s: timingDelta !== null && timingDelta <= 10,
                timing_under_60s: timingDelta !== null && timingDelta <= 60,
                timing_under_5min: timingDelta !== null && timingDelta <= 300,
                same_promo: u1.promo_code === u2.promo_code && u1.promo_code !== '',
                exposure_diff: Math.round(exposureDiff * 100) / 100,
                exposure_within_5: exposureDiff <= 5,
                exposure_in_promo_max_range: u1.user_market_exposure >= 45 && u1.user_market_exposure <= 60 && u2.user_market_exposure >= 45 && u2.user_market_exposure <= 60,
                both_balance_under_1: u1.balance < 1 && u2.balance < 1,
                both_single_trade: u1.num_trades === 1 && u2.num_trades === 1,
                users_in_market: u1.users_in_market,
                direct_counterparty: isDirectCounterparty,
                has_user_counterparty: hasUserCounterparty,
                either_flagged_in_sheet: u1.is_direct_counterparty_in_sheet || u2.is_direct_counterparty_in_sheet
              }
            });
          }
        }
      }
    }

    if (pairs.length === 0) continue;

    // Only include pairs with strong signal combinations
    // Path 1: direct_counterparty + same_promo + 1 confirming signal (strongest path)
    // Path 2: same_promo + at least 2 confirming signals (no counterparty data)
    const strongPairs = pairs.filter(p => {
      const s = p.signals;
      if (!s.same_promo) return false;

      const confirmingCount = (s.timing_under_60s ? 1 : 0)
        + (s.both_balance_under_1 ? 1 : 0)
        + (s.exposure_within_5 || s.exposure_in_promo_max_range ? 1 : 0)
        + (s.both_single_trade ? 1 : 0)
        + (s.timing_under_10s ? 1 : 0);

      // Direct counterparty = they literally traded against each other (5x signal)
      if (s.direct_counterparty) return confirmingCount >= 1;

      // No direct counterparty data — need stronger circumstantial evidence
      return confirmingCount >= 2;
    });

    if (strongPairs.length === 0) continue;

    markets.push({
      market_id: marketId,
      market_description: marketRows[0].market_description,
      total_users_in_data: marketRows.length,
      users_in_market: marketRows[0].users_in_market,
      pairs: strongPairs
    });
  }

  // Cross-market pair tracking
  const pairMarketCount = {};
  for (const m of markets) {
    for (const p of m.pairs) {
      const key = [p.user_a.user_id, p.user_b.user_id].sort().join('|');
      if (!pairMarketCount[key]) pairMarketCount[key] = new Set();
      pairMarketCount[key].add(m.market_id);
    }
  }

  for (const m of markets) {
    for (const p of m.pairs) {
      const key = [p.user_a.user_id, p.user_b.user_id].sort().join('|');
      p.signals.shared_markets = pairMarketCount[key].size;
      p.signals.repeat_offender = pairMarketCount[key].size >= 2;
    }
  }

  return markets;
}

export function parseRow(r) {
  return {
    user_id: r.user_id,
    promo_code: r.promo_code || '',
    market_id: r.market_id,
    market_description: r.market_description,
    outcome_side: r.outcome_side,
    user_market_exposure: parseFloat(r.user_market_exposure) || 0,
    num_trades: parseInt(r.num_trades) || 0,
    first_trade_in_market: r.first_trade_in_market,
    placed_at_list: r.placed_at_list,
    outcome_statuses: r.outcome_statuses,
    total_pnl: parseFloat(r.total_pnl) || 0,
    any_settled: r.any_settled,
    users_in_market: parseInt(r.users_in_market) || 0,
    balance: parseFloat(r.balance) || 0,
    counterparty_user_ids: r.counterparty_user_ids || '',
    counterparty_types: r.counterparty_types || '',
    is_direct_counterparty_in_sheet: String(r.is_direct_counterparty_in_sheet || '').toLowerCase() === 'true'
  };
}
