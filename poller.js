// ============================================
// UNIFIED POLLER — Spot Market
// Fetches: Binance Spot candles (no key needed) + account (key optional)
// Calculates: EMA20/100/200, MACD/Signal/Histogram
// Upserts to Supabase every 4 minutes
// ============================================

require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');

// ── CONFIG ──────────────────────────────────
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const BINANCE_API_KEY = process.env.BINANCE_API_KEY || '';
const BINANCE_API_SECRET = process.env.BINANCE_API_SECRET || '';
const BINANCE_BASE_URL = 'https://api.binance.com';

const POLL_INTERVAL_MS = 4 * 60 * 1000;
const TIMEFRAME = '4h';
const CANDLE_LIMIT = 500;
const BATCH_SIZE = 5;
const BATCH_DELAY_MS = 500;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_KEY');
  process.exit(1);
}

const HAS_BINANCE_KEY = BINANCE_API_KEY && BINANCE_API_SECRET;

// ── SUPABASE ────────────────────────────────
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ── MATH: EMA ───────────────────────────────
function calculateEMA(closes, period) {
  const k = 2 / (period + 1);
  let ema = closes.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < closes.length; i++) {
    ema = closes[i] * k + ema * (1 - k);
  }
  return ema;
}

// ── MATH: MACD ──────────────────────────────
function calculateMACD(closes) {
  const k12 = 2 / 13;
  const k26 = 2 / 27;
  const k9 = 2 / 10;

  let ema12 = closes.slice(0, 12).reduce((a, b) => a + b, 0) / 12;
  const ema12Series = [ema12];
  for (let i = 12; i < closes.length; i++) {
    ema12 = closes[i] * k12 + ema12 * (1 - k12);
    ema12Series.push(ema12);
  }

  let ema26 = closes.slice(0, 26).reduce((a, b) => a + b, 0) / 26;
  const ema26Series = [ema26];
  for (let i = 26; i < closes.length; i++) {
    ema26 = closes[i] * k26 + ema26 * (1 - k26);
    ema26Series.push(ema26);
  }

  const macdValues = [];
  for (let i = 25; i < closes.length; i++) {
    macdValues.push(ema12Series[i - 11] - ema26Series[i - 25]);
  }

  const macd = macdValues[macdValues.length - 1];
  let signal = macdValues.slice(0, 9).reduce((a, b) => a + b, 0) / 9;
  for (let i = 9; i < macdValues.length; i++) {
    signal = macdValues[i] * k9 + signal * (1 - k9);
  }

  return { macd, signal, histogram: macd - signal };
}

// ── MATH: ALL INDICATORS ────────────────────
function calculateAllIndicators(closes) {
  const currentClose = closes[closes.length - 1];
  const ema20 = calculateEMA(closes, 20);
  const ema100 = calculateEMA(closes, 100);
  const ema200 = calculateEMA(closes, 200);
  const macdData = calculateMACD(closes);

  return {
    close_price: currentClose,
    ema20,
    ema100,
    ema200,
    macd: macdData.macd,
    macd_signal: macdData.signal,
    macd_histogram: macdData.histogram,
    distance_ema20: ((currentClose - ema20) / ema20) * 100,
    distance_ema100: ((currentClose - ema100) / ema100) * 100,
    distance_ema200: ((currentClose - ema200) / ema200) * 100
  };
}

// ── FETCH SYMBOLS ───────────────────────────
async function getSymbols() {
  const { data, error } = await supabase
    .from('symbols')
    .select('symbol_id, symbol_name')
    .eq('is_active', true);
  if (error) { console.error('Symbols error:', error); return []; }
  return data || [];
}

// ── FETCH CANDLES (SPOT, NO KEY NEEDED) ─────
async function fetchCandles(symbolName) {
  try {
    const url = `${BINANCE_BASE_URL}/api/v3/klines?symbol=${symbolName}&interval=${TIMEFRAME}&limit=${CANDLE_LIMIT}`;
    const res = await fetch(url);
    if (!res.ok) { console.error(`[${symbolName}] HTTP ${res.status}`); return null; }
    return await res.json();
  } catch (err) {
    console.error(`[${symbolName}] Error:`, err.message);
    return null;
  }
}

// ── STORE CANDLES ───────────────────────────
async function storeCandles(symbolName, candles) {
  await supabase.from('candle_data').delete().eq('symbol_name', symbolName);
  const rows = candles.map(c => ({
    symbol_name: symbolName,
    open_time: c[0],
    open_price: parseFloat(c[1]),
    high_price: parseFloat(c[2]),
    low_price: parseFloat(c[3]),
    close_price: parseFloat(c[4]),
    volume: parseFloat(c[5]),
    close_time: c[6]
  }));
  const { error } = await supabase.from('candle_data').insert(rows);
  if (error) { console.error(`[${symbolName}] Insert failed:`, error.message); return false; }
  return true;
}

// ── PROCESS SYMBOLS ─────────────────────────
async function processSymbol(symbol) {
  const candles = await fetchCandles(symbol.symbol_name);
  if (!candles || candles.length === 0) return null;

  await storeCandles(symbol.symbol_name, candles);

  const closes = candles.map(c => parseFloat(c[4]));
  const indicators = calculateAllIndicators(closes);

  return {
    symbol_id: symbol.symbol_id,
    snapshot_time: new Date().toISOString(),
    ...indicators
  };
}

async function processSymbolsBatch(symbols) {
  const results = [];
  for (let i = 0; i < symbols.length; i += BATCH_SIZE) {
    const batch = symbols.slice(i, i + BATCH_SIZE);
    console.log(`Batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(symbols.length / BATCH_SIZE)}: ${batch.map(s => s.symbol_name).join(', ')}`);

    const batchResults = await Promise.all(batch.map(s => processSymbol(s)));
    results.push(...batchResults.filter(r => r !== null));

    if (i + BATCH_SIZE < symbols.length) await new Promise(r => setTimeout(r, BATCH_DELAY_MS));
  }
  return results;
}

// ── BINANCE SPOT ACCOUNT (KEY OPTIONAL) ─────
async function fetchBinanceSpotAccount() {
  if (!HAS_BINANCE_KEY) {
    console.log('No Binance API key — skipping account/positions fetch');
    return [];
  }

  try {
    const timestamp = Date.now();
    const queryString = `timestamp=${timestamp}`;
    const signature = crypto.createHmac('sha256', BINANCE_API_SECRET).update(queryString).digest('hex');
    const url = `${BINANCE_BASE_URL}/api/v3/account?${queryString}&signature=${signature}`;

    const response = await fetch(url, { headers: { 'X-MBX-APIKEY': BINANCE_API_KEY } });
    if (!response.ok) { console.error('Binance account error:', await response.text()); return []; }

    const data = await response.json();

    // Map balances to open_positions format
    // Filter only USDT-tradable assets with balance > 0
    const positions = [];
    for (const b of data.balances) {
      const free = parseFloat(b.free);
      const locked = parseFloat(b.locked);
      const total = free + locked;

      if (total > 0 && b.asset !== 'USDT') {
        // Try to match to our symbols (e.g. BTC -> BTCUSDT)
        const symbolName = b.asset + 'USDT';
        positions.push({
          symbol_name: symbolName,
          quantity: total,
          entry_price: 0,
          unrealized_pl: 0,
          side: 'SPOT',
          updated_at: new Date().toISOString()
        });
      }
    }
    return positions;
  } catch (err) {
    console.error('Binance account error:', err.message);
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

  try {
    const symbols = await getSymbols();
    console.log(`Symbols: ${symbols.length}`);

    const indicators = await processSymbolsBatch(symbols);
    await upsertIndicators(indicators);

    const positions = await fetchBinanceSpotAccount();
    await upsertPositions(positions);

    console.log(`Poll complete in ${((Date.now() - startTime) / 1000).toFixed(1)}s`);
  } catch (err) {
    console.error('Poll failed:', err);
  }
}

// ── START ───────────────────────────────────
async function main() {
  console.log('Spot Market Poller Started');
  console.log(`Interval: ${POLL_INTERVAL_MS / 60000} min | Candles: ${CANDLE_LIMIT} x ${TIMEFRAME}`);
  console.log(`Binance key: ${HAS_BINANCE_KEY ? 'YES (positions enabled)' : 'NO (indicators only)'}`);
  await runPoll();
  setInterval(runPoll, POLL_INTERVAL_MS);
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
