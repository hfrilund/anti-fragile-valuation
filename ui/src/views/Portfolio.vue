<template>
  <div>
    <h2>Portfolio Analytics</h2>

    <div style="display:flex;align-items:center;gap:1.5rem;margin-bottom:1.5rem;flex-wrap:wrap">
      <label style="display:flex;align-items:center;gap:0.5rem;margin:0;font-size:0.85rem">
        Window
        <select v-model="window" style="margin:0;width:auto;padding:0.2rem 0.4rem">
          <option :value="63">3 months</option>
          <option :value="126">6 months</option>
          <option :value="252">1 year</option>
          <option :value="504">2 years</option>
          <option :value="-1">YTD</option>
          <option :value="0">All time</option>
        </select>
      </label>
      <label style="display:flex;align-items:center;gap:0.5rem;margin:0;font-size:0.85rem">
        Risk-free rate
        <select v-model="rfr" style="margin:0;width:auto;padding:0.2rem 0.4rem">
          <option :value="0.00">0%</option>
          <option :value="0.02">2%</option>
          <option :value="0.03">3%</option>
          <option :value="0.04">4%</option>
          <option :value="0.05">5%</option>
        </select>
      </label>
      <span style="margin-left:auto;font-size:0.8rem;color:var(--pico-muted-color)">
        <span v-if="hoverDate" style="font-weight:600;color:#ccc">{{ hoverDate }}</span>
        <span v-else-if="coverage">{{ coverage }}</span>
      </span>
    </div>

    <div v-if="loading" aria-busy="true" style="padding:3rem 0;text-align:center;color:var(--pico-muted-color)">
      Computing rolling metrics…
    </div>
    <p v-else-if="error" style="color:var(--pico-del-color)">{{ error }}</p>
    <p v-else-if="!series.length" style="color:var(--pico-muted-color)">
      No data — save holdings and fetch price history first.
    </p>

    <template v-else>
      <div class="chart-label">Portfolio value (indexed, full history)</div>
      <div ref="valueEl" style="margin-bottom:8px"></div>

      <div class="chart-label" style="margin-top:12px">Sharpe ratio (rolling {{ windowLabel }})</div>
      <div ref="sharpeEl" style="margin-bottom:8px"></div>

      <div class="chart-label" style="margin-top:12px">Annualised return</div>
      <div ref="returnEl" style="margin-bottom:8px"></div>

      <div class="chart-label" style="margin-top:12px">Annualised volatility</div>
      <div ref="volEl" style="margin-bottom:8px"></div>

      <div class="chart-label" style="margin-top:12px">Max drawdown (within window)</div>
      <div ref="ddEl"></div>
    </template>
  </div>
</template>

<script setup>
import { ref, watch, onMounted, onUnmounted, computed } from 'vue'
import { createChart, LineSeries, AreaSeries } from 'lightweight-charts'
import { api } from '../lib/api'

const window = ref(252)
const rfr    = ref(0.04)
const series    = ref([])
const loading   = ref(false)
const error     = ref(null)
const coverage  = ref(null)
const hoverDate = ref(null)

const valueEl  = ref(null)
const sharpeEl = ref(null)
const returnEl = ref(null)
const volEl    = ref(null)
const ddEl     = ref(null)

let charts = []
let ro = null

function ytdFromDate() {
  return new Date(new Date().getFullYear(), 0, 1).toISOString().split('T')[0]
}

function ytdTradingDays() {
  const daysSinceJan1 = Math.floor((Date.now() - new Date(new Date().getFullYear(), 0, 1)) / 86400000)
  return Math.max(30, Math.round(daysSinceJan1 * 252 / 365))
}

const windowLabel = computed(() => {
  const map = { '-1': 'YTD', 0: 'all time', 63: '3 months', 126: '6 months', 252: '1 year', 504: '2 years' }
  return map[window.value] ?? `${window.value} days`
})

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

function syncTimescales(...cs) {
  let syncing = false
  cs.forEach(src => {
    src.timeScale().subscribeVisibleLogicalRangeChange(range => {
      if (syncing || !range) return
      syncing = true
      cs.forEach(t => { if (t !== src) t.timeScale().setVisibleLogicalRange(range) })
      syncing = false
    })
  })
}

function destroyCharts() {
  charts.forEach(c => c.remove())
  charts = []
  ro?.disconnect()
  ro = null
  hoverDate.value = null
}

function pctFmt(n) {
  return (n * 100).toFixed(1) + '%'
}

function build(data) {
  destroyCharts()

  const valueData  = data.map(d => ({ time: d.date, value: d.portfolio_value }))
  const sharpeData = data.map(d => ({ time: d.date, value: d.sharpe }))
  const returnData = data.map(d => ({ time: d.date, value: d.ann_return }))
  const volData    = data.map(d => ({ time: d.date, value: d.ann_vol }))
  const ddData     = data.map(d => ({ time: d.date, value: d.max_drawdown }))

  const w = valueEl.value.clientWidth

  // ── Portfolio value chart ──
  const vc2 = createChart(valueEl.value, { ...baseOptions(220), width: w })
  vc2.addSeries(AreaSeries, {
    lineColor: '#42a5f5', topColor: '#42a5f533', bottomColor: '#42a5f500',
    lineWidth: 2, lastValueVisible: true, priceLineVisible: false, title: 'Value',
    priceFormat: { type: 'price', precision: 3, minMove: 0.001 },
  }).setData(valueData)
  vc2.timeScale().fitContent()
  charts.push(vc2)

  // ── Sharpe chart ──
  const sc = createChart(sharpeEl.value, { ...baseOptions(220), width: w })
  const ss = sc.addSeries(LineSeries, {
    color: '#ce93d8', lineWidth: 2, lastValueVisible: true, priceLineVisible: false, title: 'Sharpe',
    priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
  })
  ss.setData(sharpeData)
  ss.createPriceLine({ price: 1,  color: '#26a69a', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '1.0' })
  ss.createPriceLine({ price: 0,  color: '#555',    lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '0' })
  sc.timeScale().fitContent()
  charts.push(sc)

  // ── Return chart ──
  const rc = createChart(returnEl.value, { ...baseOptions(180), width: w })
  const rs = rc.addSeries(LineSeries, {
    color: '#4caf50', lineWidth: 2, lastValueVisible: true, priceLineVisible: false, title: 'Return',
    priceFormat: { type: 'percent', precision: 1, minMove: 0.1 },
  })
  rs.setData(returnData.map(d => ({ ...d, value: d.value * 100 })))
  rs.createPriceLine({ price: 0, color: '#555', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '0%' })
  rc.timeScale().fitContent()
  charts.push(rc)

  // ── Volatility chart ──
  const vc = createChart(volEl.value, { ...baseOptions(160), width: w })
  vc.addSeries(LineSeries, {
    color: '#ff9800', lineWidth: 2, lastValueVisible: true, priceLineVisible: false, title: 'Vol',
    priceFormat: { type: 'percent', precision: 1, minMove: 0.1 },
  }).setData(volData.map(d => ({ ...d, value: d.value * 100 })))
  vc.timeScale().fitContent()
  charts.push(vc)

  // ── Drawdown chart (area, always ≤ 0) ──
  const dc = createChart(ddEl.value, { ...baseOptions(160), width: w })
  dc.addSeries(AreaSeries, {
    lineColor: '#ef5350', topColor: '#ef535033', bottomColor: '#ef535000',
    lineWidth: 2, lastValueVisible: true, priceLineVisible: false, title: 'DD',
    priceFormat: { type: 'percent', precision: 1, minMove: 0.1 },
  }).setData(ddData.map(d => ({ ...d, value: d.value * 100 })))
  dc.timeScale().fitContent()
  charts.push(dc)

  syncTimescales(...charts)

  charts.forEach(c => {
    c.subscribeCrosshairMove(param => {
      if (param.time) {
        const d = new Date(param.time)
        hoverDate.value = d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' })
      } else {
        hoverDate.value = null
      }
    })
  })

  if (window.value !== 0) {
    const lastDate = data[data.length - 1].date
    const fromDate = window.value === -1
      ? ytdFromDate()
      : (() => { const d = new Date(lastDate); d.setDate(d.getDate() - Math.round(window.value * 365 / 252)); return d.toISOString().split('T')[0] })()
    charts[0].timeScale().setVisibleRange({ from: fromDate, to: lastDate })
  }

  ro = new ResizeObserver(() => {
    const cw = sharpeEl.value?.clientWidth ?? 0
    charts.forEach(c => c.applyOptions({ width: cw }))
  })
  ro.observe(sharpeEl.value)
}

async function load() {
  loading.value = true
  error.value = null
  series.value = []
  coverage.value = null
  destroyCharts()
  try {
    const apiWindow = window.value === 0 ? 504 : window.value === -1 ? ytdTradingDays() : window.value
    const data = await api.dashboard.sharpeHistory({ window: apiWindow, risk_free_rate: rfr.value })
    if (!data || !data.length) {
      loading.value = false
      return
    }
    series.value = data
    loading.value = false
    await new Promise(r => setTimeout(r, 0))  // let DOM render chart containers
    build(data)
    coverage.value = `${data.length} data points  ·  ${data[0].date} → ${data[data.length - 1].date}`
  } catch (e) {
    loading.value = false
    error.value = e.message
  }
}

onMounted(load)
watch([window, rfr], load)
onUnmounted(destroyCharts)
</script>

<style scoped>
.chart-label {
  font-size: 0.85rem;
  font-weight: 600;
  color: #ccc;
  padding: 0 4px 2px;
}
</style>
