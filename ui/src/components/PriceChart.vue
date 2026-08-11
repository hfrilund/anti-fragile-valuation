<template>
  <div>
    <div v-if="loading" style="padding:2rem;text-align:center;color:var(--pico-muted-color)">
      Loading price data…
    </div>
    <div v-else-if="err" style="color:var(--pico-del-color);padding:1rem">{{ err }}</div>

    <template v-else>
      <!-- Price + volume -->
      <div style="position:relative">
        <div v-if="tip" class="chart-tip">
          <span class="tip-date">{{ tip.date }}</span>
          <span>O <b>{{ f(tip.open) }}</b></span>
          <span>H <b>{{ f(tip.high) }}</b></span>
          <span>L <b>{{ f(tip.low) }}</b></span>
          <span>C <b :style="{ color: tip.close >= tip.open ? '#26a69a' : '#ef5350' }">{{ f(tip.close) }}</b></span>
          <span>Vol <b>{{ fVol(tip.volume) }}</b></span>
          <span v-if="tip.rsi != null">RSI <b>{{ tip.rsi.toFixed(1) }}</b></span>
          <span v-if="tip.macd != null">MACD <b :style="{ color: tip.macd >= 0 ? '#26a69a' : '#ef5350' }">{{ tip.macd.toFixed(3) }}</b></span>
          <span v-if="tip.signal != null">Sig <b>{{ tip.signal.toFixed(3) }}</b></span>
        </div>
        <div ref="priceEl"></div>
      </div>

      <!-- RSI -->
      <div class="chart-label" style="margin-top:6px">RSI (14)</div>
      <div ref="rsiEl"></div>

      <!-- MACD -->
      <div class="chart-label" style="margin-top:6px">MACD (12, 26, 9)</div>
      <div ref="macdEl"></div>

      <!-- OBV -->
      <div class="chart-label" style="margin-top:6px">OBV</div>
      <div ref="obvEl"></div>
    </template>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, watch } from 'vue'
import {
  createChart,
  CandlestickSeries,
  LineSeries,
  HistogramSeries,
} from 'lightweight-charts'
import { api } from '../lib/api'

const props = defineProps({ symbol: String })

const priceEl = ref(null)
const rsiEl   = ref(null)
const macdEl  = ref(null)
const obvEl   = ref(null)
const loading = ref(true)
const err     = ref(null)
const tip     = ref(null)

let priceChart = null
let rsiChart   = null
let macdChart  = null
let obvChart   = null
let ro         = null

// -- formatting --

function f(n) {
  return n == null ? '—' : n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}
function fVol(n) {
  if (n == null) return '—'
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return n.toString()
}

// -- indicators --

function computeMA(data, period) {
  const result = []
  for (let i = period - 1; i < data.length; i++) {
    let sum = 0
    for (let j = i - period + 1; j <= i; j++) sum += data[j].close
    result.push({ time: data[i].time, value: sum / period })
  }
  return result
}

function computeRSI(data, period = 14) {
  if (data.length <= period) return []
  const result = []
  let avgGain = 0, avgLoss = 0
  for (let i = 1; i <= period; i++) {
    const d = data[i].close - data[i - 1].close
    avgGain += Math.max(d, 0)
    avgLoss += Math.max(-d, 0)
  }
  avgGain /= period; avgLoss /= period
  result.push({ time: data[period].time, value: 100 - 100 / (1 + avgGain / (avgLoss || 1e-10)) })
  for (let i = period + 1; i < data.length; i++) {
    const d = data[i].close - data[i - 1].close
    avgGain = (avgGain * (period - 1) + Math.max(d, 0)) / period
    avgLoss = (avgLoss * (period - 1) + Math.max(-d, 0)) / period
    result.push({ time: data[i].time, value: 100 - 100 / (1 + avgGain / (avgLoss || 1e-10)) })
  }
  return result
}

function computeMACD(data, fast = 12, slow = 26, signal = 9) {
  if (data.length < slow + signal) return { macd: [], signal: [], hist: [] }
  function ema(values, period) {
    const k = 2 / (period + 1)
    const out = [values[0]]
    for (let i = 1; i < values.length; i++)
      out.push(values[i] * k + out[i - 1] * (1 - k))
    return out
  }
  const closes  = data.map(d => d.close)
  const macdRaw = ema(closes, fast).map((v, i) => v - ema(closes, slow)[i])
  const sigRaw  = ema(macdRaw.slice(slow - 1), signal)
  const offset  = slow - 1 + signal - 1
  const macdOut = [], signalOut = [], histOut = []
  for (let i = offset; i < data.length; i++) {
    const m = macdRaw[i], s = sigRaw[i - (slow - 1)], h = m - s
    macdOut.push({ time: data[i].time, value: m })
    signalOut.push({ time: data[i].time, value: s })
    histOut.push({ time: data[i].time, value: h, color: h >= 0 ? '#26a69a66' : '#ef535066' })
  }
  return { macd: macdOut, signal: signalOut, hist: histOut }
}

function computeOBV(rawData) {
  const result = []
  let obv = 0
  for (let i = 0; i < rawData.length; i++) {
    if (i > 0) {
      if (rawData[i].close > rawData[i - 1].close)      obv += rawData[i].volume
      else if (rawData[i].close < rawData[i - 1].close) obv -= rawData[i].volume
    }
    result.push({ time: rawData[i].date, value: obv })
  }
  return result
}

// -- shared chart options --

function baseOptions(height) {
  return {
    layout: { background: { color: 'transparent' }, textColor: '#9e9e9e' },
    grid: { vertLines: { color: '#2a2a2a' }, horzLines: { color: '#2a2a2a' } },
    crosshair: { mode: 1 },
    timeScale: { borderColor: '#3a3a3a', visible: false },
    rightPriceScale: { borderColor: '#3a3a3a' },
    height,
  }
}

// -- time range sync (bidirectional, loop-guarded) --

function syncTimescales(...charts) {
  let syncing = false
  charts.forEach(source => {
    source.timeScale().subscribeVisibleLogicalRangeChange(range => {
      if (syncing || !range) return
      syncing = true
      charts.forEach(target => {
        if (target !== source) target.timeScale().setVisibleLogicalRange(range)
      })
      syncing = false
    })
  })
}

// -- build --

function build(rawData) {
  const clean = rawData.filter(
    d => d.open != null && d.high != null && d.low != null && d.close != null && d.close > 0
  )
  if (!clean.length) {
    err.value = 'No valid price data available.'
    return
  }

  const mapped = clean.map(d => ({
    time: d.date, open: d.open, high: d.high, low: d.low, close: d.close,
  }))

  // index for tooltip lookup
  const idx = {}, rsiIdx = {}, macdIdx = {}, signalIdx = {}
  for (const d of rawData) idx[d.date] = d

  const w = priceEl.value.clientWidth

  // ---- Price chart ----
  priceChart = createChart(priceEl.value, { ...baseOptions(360), timeScale: { borderColor: '#3a3a3a', visible: false } })

  const candles = priceChart.addSeries(CandlestickSeries, {
    upColor: '#26a69a', downColor: '#ef5350',
    borderVisible: false, wickUpColor: '#26a69a', wickDownColor: '#ef5350',
  })
  candles.setData(mapped)

  priceChart.addSeries(LineSeries, {
    color: '#ff9800', lineWidth: 1, lastValueVisible: false, priceLineVisible: false, title: 'MA50',
  }).setData(computeMA(mapped, 50))

  priceChart.addSeries(LineSeries, {
    color: '#2196f3', lineWidth: 1, lastValueVisible: false, priceLineVisible: false, title: 'MA200',
  }).setData(computeMA(mapped, 200))

  // Volume as overlay in bottom 20% of price chart
  const vol = priceChart.addSeries(HistogramSeries, {
    priceFormat: { type: 'volume' }, priceScaleId: '',
    lastValueVisible: false, priceLineVisible: false,
  })
  vol.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } })
  vol.setData(clean.filter(d => d.volume != null).map(d => ({
    time: d.date, value: d.volume,
    color: d.close >= d.open ? '#26a69a44' : '#ef535044',
  })))

  priceChart.timeScale().fitContent()

  // Crosshair tooltip (price chart only)
  priceChart.subscribeCrosshairMove(param => {
    if (!param.time) { tip.value = null; return }
    const date = typeof param.time === 'string'
      ? param.time
      : new Date(param.time * 1000).toISOString().slice(0, 10)
    const row = idx[date]
    if (!row) { tip.value = null; return }
    tip.value = {
      date, open: row.open, high: row.high, low: row.low, close: row.close, volume: row.volume,
      rsi: rsiIdx[date] ?? null, macd: macdIdx[date] ?? null, signal: signalIdx[date] ?? null,
    }
  })

  // ---- RSI chart ----
  rsiChart = createChart(rsiEl.value, { ...baseOptions(200), timeScale: { borderColor: '#3a3a3a', visible: false } })

  const rsiData = computeRSI(mapped)
  for (const r of rsiData) rsiIdx[r.time] = r.value

  const rsiSeries = rsiChart.addSeries(LineSeries, {
    color: '#ce93d8', lineWidth: 1, lastValueVisible: true, priceLineVisible: false, title: 'RSI14',
  })
  rsiSeries.setData(rsiData)
  rsiSeries.createPriceLine({ price: 70, color: '#ef5350', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '70' })
  rsiSeries.createPriceLine({ price: 50, color: '#555',    lineWidth: 1, lineStyle: 2, axisLabelVisible: false })
  rsiSeries.createPriceLine({ price: 30, color: '#26a69a', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '30' })

  // ---- MACD chart ----
  macdChart = createChart(macdEl.value, { ...baseOptions(180) })

  const macdData = computeMACD(mapped)
  for (const r of macdData.macd)   macdIdx[r.time]   = r.value
  for (const r of macdData.signal) signalIdx[r.time] = r.value

  macdChart.addSeries(HistogramSeries, {
    lastValueVisible: false, priceLineVisible: false,
  }).setData(macdData.hist)

  macdChart.addSeries(LineSeries, {
    color: '#42a5f5', lineWidth: 1, lastValueVisible: true, priceLineVisible: false, title: 'MACD',
  }).setData(macdData.macd)

  macdChart.addSeries(LineSeries, {
    color: '#ef9a9a', lineWidth: 1, lastValueVisible: false, priceLineVisible: false, title: 'Sig',
  }).setData(macdData.signal)

  // ---- OBV chart ----
  obvChart = createChart(obvEl.value, { ...baseOptions(150) })

  obvChart.addSeries(LineSeries, {
    color: '#00bcd4', lineWidth: 1, lastValueVisible: true, priceLineVisible: false, title: 'OBV',
    priceFormat: { type: 'price', precision: 0, minMove: 1 },
  }).setData(computeOBV(clean))

  // Sync all four time scales
  syncTimescales(priceChart, rsiChart, macdChart, obvChart)

  // Resize observer
  ro = new ResizeObserver(() => {
    const cw = priceEl.value?.clientWidth ?? 0
    priceChart?.applyOptions({ width: cw })
    rsiChart?.applyOptions({ width: cw })
    macdChart?.applyOptions({ width: cw })
    obvChart?.applyOptions({ width: cw })
  })
  ro.observe(priceEl.value)
}

// -- data load --

async function load(sym) {
  loading.value = true
  err.value = null
  priceChart?.remove(); rsiChart?.remove(); macdChart?.remove(); obvChart?.remove()
  priceChart = rsiChart = macdChart = obvChart = null
  ro?.disconnect()
  try {
    const data = await api.prices.history(sym)
    loading.value = false
    await new Promise(r => setTimeout(r, 0))
    build(data)
  } catch (e) {
    loading.value = false
    err.value = e.message
  }
}

onMounted(() => { if (props.symbol) load(props.symbol) })
watch(() => props.symbol, sym => { if (sym) load(sym) })
onUnmounted(() => {
  priceChart?.remove(); rsiChart?.remove(); macdChart?.remove(); obvChart?.remove()
  ro?.disconnect()
})
</script>

<style scoped>
.chart-tip {
  position: absolute;
  top: 8px;
  left: 8px;
  z-index: 10;
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem 1rem;
  padding: 4px 10px;
  background: rgba(18, 18, 18, 0.82);
  border-radius: 4px;
  font-size: 0.78rem;
  color: #ccc;
  pointer-events: none;
  backdrop-filter: blur(4px);
}
.tip-date {
  color: #9e9e9e;
  margin-right: 0.25rem;
}
.chart-label {
  font-size: 0.85rem;
  font-weight: 600;
  color: #ccc;
  padding: 0 4px 2px;
}
</style>
