// ============================================
// UNIFIED POLLER — Railway
// Fetches: TradingView indicators + Binance positions
// Upserts to Supabase every 4 minutes
// ============================================

require('dotenv').config();
const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StdioClientTransport } = require('@modelcontextprotocol/sdk/client/stdio.js');
const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');

// ── CONFIG ──────────────────────────────────
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const MCP_SERVER_PATH = './tv-mcp/build/index.js'; // Cloned during Railway build

const BINANCE_API_KEY = process.env.BINANCE_API_KEY;
const BINANCE_API_SECRET = process.env.BINANCE_API_SECRET;
const BINANCE_BASE_URL = 'https://fapi.binance.com'; // USD-M Futures

const POLL_INTERVAL_MS = 4 * 60 * 1000; // 4 minutes
const TIMEFRAME = '4h';
const BATCH_SIZE = 5;
const BATCH_DELAY_MS = 500;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !BINANCE_API_KEY || !BINANCE_API_SECRET) {
  console.error('Missing env vars. Need: SUPABASE_URL, SUPABASE_SERVICE_KEY, BINANCE_API_KEY, BINANCE_API_SECRET');
  process.exit(1);
}

// ── SUPABASE ────────────────────────────────
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ── FETCH SYMBOLS FROM SUPABASE ─────────────
async function getSymbols() {
  const { data, error } = await supabase
    .from('symbols')
    .select('symbol_id, symbol_name')
    .eq('is_active', true);

  if (error) {
    console.error('Failed to fetch symbols:', error);
    return [];
  }
  return data || [];
}

// ── TRADINGVIEW MCP ─────────────────────────
async function createTvMcpClient() {
  const transport = new StdioClientTransport({
    command: 'node',
    args: [MCP_SERVER_PATH]
  });
  const client = new Client({ name: 'dashboard-poller', version: '1.0' });
  await client.connect(transport);
  return { client, transport };
}

async function fetchTvIndicators(mcpClient, symbol) {
  try {
    const tvSymbol = `BINANCE:${symbol.symbol_name}`;

    const result = await mcpClient.callTool({
      name: 'get_indicator_values',
      arguments: {
        symbol: tvSymbol,
        timeframe: TIMEFRAME,
        indicators: ['EMA20', 'EMA100', 'EMA200', 'MACD.macd', 'MACD.signal']
      }
    });

    const rawText = result.content?.[0]?.text || '{}';
    let raw;
    try { raw = JSON.parse(rawText); } catch (e) { return null; }

    const indicators = raw.indicators || raw;

    const closePrice = parseFloat(raw.close || raw.price || indicators.close || indicators.price);
    const ema20 = parseFloat(indicators.EMA20 || indicators.ema20);
    const ema100 = parseFloat(indicators.EMA100 || indicators.ema100);
    const ema200 = parseFloat(indicators.EMA200 || indicators.ema200);
    const macd = parseFloat(indicators['MACD.macd'] || indicators.MACD || indicators.macd);
    const macdSignal = parseFloat(indicators['MACD.signal'] || indicators.MACD_SIGNAL || indicators.macd_signal);

    if ([closePrice, ema20, ema100, ema200, macd, macdSignal].some(v => isNaN(v))) {
      console.warn(`[${symbol.symbol_name}] Missing indicator data`);
      return null;
    }

    return {
      symbol_id: symbol.symbol_id,
      snapshot_time: new Date().toISOString(),
      close_price: closePrice,
      ema20,
      ema100,
      ema200,
      macd,
      macd_signal: macdSignal,
      macd_histogram: macd - macdSignal,
      distance_ema20: ((closePrice - ema20) / ema20) * 100,
      distance_ema100: ((closePrice - ema100) / ema100) * 100,
      distance_ema200: ((closePrice - ema200) / ema200) * 100
    };
  } catch (err) {
    console.error(`[${symbol.symbol_name}] TV fetch failed:`, err.message);
    return null;
  }
}

async function processTvBatch(mcpClient, symbols) {
  const results = [];
  for (let i = 0; i < symbols.length; i += BATCH_SIZE) {
    const batch = symbols.slice(i, i + BATCH_SIZE);
    const batchResults = await Promise.all(batch.map(s => fetchTvIndicators(mcpClient, s)));
    results.push(...batchResults.filter(r => r !== null));
    if (i + BATCH_SIZE < symbols.length) await new Promise(r => setTimeout(r, BATCH_DELAY_MS));
  }
  return results;
}

// ── BINANCE POSITIONS (REST API) ────────────
async function fetchBinancePositions() {
  try {
    const timestamp = Date.now();
    const queryString = `timestamp=${timestamp}`;
    const signature = crypto.createHmac('sha256', BINANCE_API_SECRET).update(queryString).digest('hex');

    const url = `${BINANCE_BASE_URL}/fapi/v2/positionRisk?${queryString}&signature=${signature}`;
    const response = await fetch(url, { headers: { 'X-MBX-APIKEY': BINANCE_API_KEY } });

    if (!response.ok) {
      console.error('Binance positions fetch failed:', await response.text());
      return [];
    }

    const data = await response.json();
    return data
      .filter(pos => parseFloat(pos.positionAmt) !== 0)
      .map(pos => ({
        symbol_name: pos.symbol,
        quantity: parseFloat(pos.positionAmt),
        entry_price: parseFloat(pos.entryPrice),
        unrealized_pl: parseFloat(pos.unRealizedProfit),
        side: pos.positionSide === 'BOTH' ? (parseFloat(pos.positionAmt) > 0 ? 'LONG' : 'SHORT') : pos.positionSide,
        updated_at: new Date().toISOString()
      }));
  } catch (err) {
    console.error('Binance positions error:', err.message);
    return [];
  }
}

// ── WRITE TO SUPABASE ───────────────────────
async function upsertIndicators(payloads) {
  if (payloads.length === 0) return;
  const { error } = await supabase.from('indicator_snapshots').upsert(payloads, { onConflict: 'symbol_id' });
  if (error) throw error;
  console.log(`[${new Date().toISOString()}] Upserted ${payloads.length} indicators`);
}

async function upsertPositions(positions) {
  await supabase.from('open_positions').delete().neq('symbol_name', '___dummy___');
  if (positions.length > 0) {
    const { error } = await supabase.from('open_positions').upsert(positions, { onConflict: 'symbol_name' });
    if (error) throw error;
  }
  console.log(`[${new Date().toISOString()}] Upserted ${positions.length} positions`);
}

// ── MAIN POLL LOOP ──────────────────────────
async function runPoll() {
  const startTime = Date.now();
  console.log(`\n========== POLL START ${new Date().toISOString()} ==========`);

  let tvMcp, tvTransport;
  try {
    const tv = await createTvMcpClient();
    tvMcp = tv.client;
    tvTransport = tv.transport;

    const symbols = await getSymbols();
    console.log(`Symbols: ${symbols.length} | ${symbols.map(s => s.symbol_name).join(', ')}`);

    const indicators = await processTvBatch(tvMcp, symbols);
    await upsertIndicators(indicators);

    const positions = await fetchBinancePositions();
    await upsertPositions(positions);

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`Poll complete in ${duration}s. Next: ${POLL_INTERVAL_MS / 60000} min.`);
  } catch (err) {
    console.error('Poll failed:', err);
  } finally {
    if (tvTransport) try { await tvTransport.close(); } catch (e) {}
  }
}

// ── START ───────────────────────────────────
async function main() {
  console.log('Unified Poller Started');
  console.log(`Interval: ${POLL_INTERVAL_MS / 60000} min | Timeframe: ${TIMEFRAME}`);
  await runPoll();
  setInterval(runPoll, POLL_INTERVAL_MS);
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
