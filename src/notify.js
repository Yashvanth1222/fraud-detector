export async function sendToN8N(confirmedMarkets, allRows) {
  const webhookUrl = process.env.N8N_WEBHOOK_URL;
  if (!webhookUrl) {
    console.error('N8N_WEBHOOK_URL not set');
    return;
  }

  for (const market of confirmedMarkets) {
    const flaggedIds = new Set(market.user_ids || []);
    const flaggedRows = allRows.filter(r =>
      flaggedIds.has(r.user_id) && r.market_id === market.market_id
    );

    const payload = {
      market_description: market.market_description,
      market_id: market.market_id,
      user_ids: market.user_ids,
      reasoning: market.reasoning,
      flagged_rows: flaggedRows
    };

    try {
      const res = await fetch(webhookUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!res.ok) {
        console.error(`n8n webhook failed (${res.status}):`, await res.text());
      } else {
        console.log(`Sent alert for market: ${market.market_description}`);
      }
    } catch (e) {
      console.error('n8n webhook error:', e.message);
    }
  }
}
