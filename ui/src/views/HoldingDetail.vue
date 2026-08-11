<template>
  <div>
    <div style="margin-bottom:1.5rem">
      <a href="#" @click.prevent="$router.back()" style="font-size:0.85rem;color:var(--pico-muted-color)">← Back</a>
    </div>

    <div v-if="error" style="color:var(--pico-del-color)">{{ error }}</div>
    <div v-else-if="!holding" aria-busy="true">Loading…</div>

    <template v-else>
      <h2 style="margin-bottom:0.25rem">{{ holding.symbol }}</h2>
      <p style="color:var(--pico-muted-color);margin-bottom:2rem">
        {{ holding.description }}
        <span v-if="holding.isin" style="margin-left:1rem;font-size:0.8rem">{{ holding.isin }}</span>
      </p>

      <!-- Investment thesis -->
      <article style="margin:0 0 2rem">
        <header>
          <strong>Investment thesis</strong>
          <span v-if="holding.thesis_updated_at" style="float:right;font-size:0.75rem;color:var(--pico-muted-color)">
            Last saved {{ fmtDate(holding.thesis_updated_at) }}
          </span>
        </header>
        <textarea
          v-model="thesis"
          rows="8"
          placeholder="Why do you own this? What would make you sell?"
          style="width:100%;resize:vertical;font-size:0.9rem"
        ></textarea>
        <footer style="display:flex;align-items:center;gap:1rem">
          <button @click="saveThesis" :aria-busy="saving" style="margin:0;width:auto">
            {{ saving ? '' : 'Save' }}
          </button>
          <span v-if="savedMsg" style="font-size:0.85rem;color:var(--pico-ins-color)">{{ savedMsg }}</span>
          <span v-if="saveError" style="font-size:0.85rem;color:var(--pico-del-color)">{{ saveError }}</span>
        </footer>
      </article>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:1.5rem;margin-bottom:2rem">
        <!-- Holdings card -->
        <article style="margin:0">
          <header><strong>Position</strong></header>
          <table style="margin:0">
            <tbody>
              <tr><td>Shares</td><td>{{ holding.position }}</td></tr>
              <tr><td>Mark price</td><td>{{ holding.currency }} {{ fmt(holding.mark_price, 2) }}</td></tr>
              <tr><td>Position value</td><td>{{ holding.currency }} {{ fmt(holding.pos_value, 0) }}</td></tr>
              <tr><td>Cost basis</td><td>{{ holding.currency }} {{ fmt(holding.cost_basis, 0) }}</td></tr>
              <tr><td>Cost per share</td><td>{{ holding.currency }} {{ fmt(holding.cost_per_share, 2) }}</td></tr>
              <tr>
                <td>P&L</td>
                <td :class="holding.pnl >= 0 ? 'pnl-pos' : 'pnl-neg'">
                  {{ holding.currency }} {{ fmt(holding.pnl, 0) }}
                  ({{ holding.pnl_pct?.toFixed(1) }}%)
                </td>
              </tr>
              <tr><td>Exchange</td><td>{{ holding.exchange }}</td></tr>
            </tbody>
          </table>
          <footer style="font-size:0.75rem;color:var(--pico-muted-color)">
            As of {{ fmtDate(holding.fetched_at) }}
          </footer>
        </article>

        <!-- AFV21 score card -->
        <article style="margin:0">
          <header>
            <strong>AFV21 Score</strong>
            <span style="float:right;font-size:1.4rem;font-weight:700">{{ holding.afv21?.toFixed(2) }}</span>
          </header>
          <table style="margin:0">
            <tbody>
              <tr><td>Return potential (RP21)</td><td>{{ holding.rp21?.toFixed(2) }}</td></tr>
              <tr><td>FCF yield</td><td>{{ pct(holding.fcf_yield) }}</td></tr>
              <tr><td>OCF margin</td><td>{{ pct(holding.ocf_margin) }}</td></tr>
              <tr><td>Min OCF margin</td><td>{{ pct(holding.min_ocf_margin) }}</td></tr>
              <tr><td>OCF margin volatility</td><td>{{ pct(holding.ocf_margin_volatility) }}</td></tr>
              <tr><td style="padding-top:0.75rem;color:var(--pico-muted-color)">Sector</td><td>{{ holding.sector_score?.toFixed(2) }}</td></tr>
              <tr><td style="color:var(--pico-muted-color)">Geography</td><td>{{ holding.geo_score?.toFixed(2) }}</td></tr>
              <tr><td style="color:var(--pico-muted-color)">Debt</td><td>{{ holding.debt_score?.toFixed(2) }}</td></tr>
              <tr><td style="color:var(--pico-muted-color)">Trend</td><td>{{ holding.trend_score?.toFixed(2) }}</td></tr>
              <tr><td style="color:var(--pico-muted-color)">VD</td><td>{{ holding.vd_score?.toFixed(2) }}</td></tr>
            </tbody>
          </table>
          <footer style="font-size:0.75rem;color:var(--pico-muted-color)">
            Scored {{ fmtDate(holding.score_computed_at) }}
          </footer>
        </article>
      </div>

      <!-- Score history chart -->
      <article style="margin:0 0 1.5rem">
        <header><strong>AFV21 Score History</strong></header>
        <div v-if="!history.length" style="color:var(--pico-muted-color);padding:1rem">No history available.</div>
        <div v-else ref="chartEl" style="height:280px"></div>
      </article>

      <!-- Price chart -->
      <article v-if="holding.yahoo_ticker" style="margin:0 0 1.5rem">
        <header>
          <strong>Price — {{ holding.yahoo_ticker }}</strong>
          <span style="float:right;font-size:0.75rem;color:var(--pico-muted-color)">
            <span style="color:#ff9800">— MA50</span>&ensp;
            <span style="color:#2196f3">— MA200</span>&ensp;
            <span style="color:#ce93d8">RSI14</span>&ensp;
            <span style="color:#42a5f5">MACD</span>/<span style="color:#ef9a9a">Sig</span>
          </span>
        </header>
        <PriceChart :symbol="holding.yahoo_ticker" />
      </article>

    </template>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import { createChart, LineSeries } from 'lightweight-charts'
import { api } from '../lib/api'
import PriceChart from '../components/PriceChart.vue'

const route = useRoute()
const holding = ref(null)
const history = ref([])
const error = ref(null)
const chartEl = ref(null)
let chart = null

const thesis = ref('')
const saving = ref(false)
const savedMsg = ref('')
const saveError = ref('')

function fmt(n, decimals = 2) {
  if (n == null) return '—'
  return n.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals })
}
function pct(n) {
  if (n == null) return '—'
  return (n * 100).toFixed(1) + '%'
}
function fmtDate(s) {
  if (!s) return '—'
  return new Date(s).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' })
}

function buildChart() {
  if (!chartEl.value || !history.value.length) return

  chart = createChart(chartEl.value, {
    layout: {
      background: { color: 'transparent' },
      textColor: '#9e9e9e',
    },
    grid: {
      vertLines: { color: '#2a2a2a' },
      horzLines: { color: '#2a2a2a' },
    },
    timeScale: { borderColor: '#3a3a3a' },
    rightPriceScale: { borderColor: '#3a3a3a' },
    height: 280,
  })

  const series = chart.addSeries(LineSeries, {
    color: '#4caf50',
    lineWidth: 2,
    priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
  })

  series.setData(
    history.value.map(r => ({ time: r.date, value: r.afv21 }))
  )

  chart.timeScale().fitContent()

  const ro = new ResizeObserver(() => {
    chart?.applyOptions({ width: chartEl.value?.clientWidth })
  })
  ro.observe(chartEl.value)
}

async function saveThesis() {
  saving.value = true
  savedMsg.value = ''
  saveError.value = ''
  try {
    await api.holdings.saveThesis(holding.value.symbol, thesis.value)
    holding.value.thesis_updated_at = new Date().toISOString()
    savedMsg.value = 'Saved'
    setTimeout(() => { savedMsg.value = '' }, 3000)
  } catch (e) {
    saveError.value = e.message
  } finally {
    saving.value = false
  }
}

onMounted(async () => {
  const symbol = route.params.symbol
  try {
    const detail = await api.holdings.detail(symbol)
    holding.value = detail
    thesis.value = detail.thesis ?? ''

    if (detail.yahoo_ticker) {
      const hist = await api.scores.history(detail.yahoo_ticker)
      history.value = hist
    }
  } catch (e) {
    error.value = e.message
  }
})

watch(history, () => {
  if (history.value.length && chartEl.value) {
    buildChart()
  }
}, { flush: 'post' })

onUnmounted(() => {
  chart?.remove()
})
</script>
