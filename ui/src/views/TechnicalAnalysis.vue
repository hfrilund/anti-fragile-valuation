<template>
  <div>
    <div style="margin-bottom:1.5rem">
      <a href="#" @click.prevent="$router.back()" style="font-size:0.85rem;color:var(--pico-muted-color)">← {{ backLabel }}</a>
    </div>

    <div v-if="error" style="color:var(--pico-del-color)">{{ error }}</div>
    <div v-else-if="loading" aria-busy="true">Loading…</div>

    <template v-else>
      <h2 style="margin-bottom:0.25rem">{{ symbol }}</h2>
      <p style="color:var(--pico-muted-color);margin-bottom:2rem">{{ score?.asset_name ?? '—' }}</p>

      <div style="display:grid;grid-template-columns:1fr 1fr;gap:1.5rem;margin-bottom:2rem">
        <!-- AFV21 score card -->
        <article style="margin:0">
          <header>
            <strong>AFV21 Score</strong>
            <span style="float:right;font-size:1.4rem;font-weight:700">{{ score?.afv21?.toFixed(2) ?? '—' }}</span>
          </header>
          <div v-if="!score" style="color:var(--pico-muted-color);padding:0.5rem 0">No score data available.</div>
          <table v-else style="margin:0">
            <tbody>
              <tr v-if="score.sector"><td>Sector</td><td>{{ score.sector }}</td></tr>
              <tr v-if="score.industry"><td>Industry</td><td>{{ score.industry }}</td></tr>
              <tr v-if="score.market_cap != null"><td>Market cap</td><td>{{ fmtCap(score.market_cap) }}</td></tr>
              <tr v-if="score.avg_volume_3m != null"><td>Avg volume (3m)</td><td>{{ fmtVol(score.avg_volume_3m) }}</td></tr>
              <tr><td style="padding-top:0.75rem">Return potential (RP21)</td><td>{{ score.rp21?.toFixed(2) ?? '—' }}</td></tr>
              <tr><td>FCF yield</td><td>{{ pct(score.fcf_yield) }}</td></tr>
              <tr><td>OCF margin</td><td>{{ pct(score.ocf_margin) }}</td></tr>
              <tr><td>Min OCF margin</td><td>{{ pct(score.min_ocf_margin) }}</td></tr>
              <tr><td>OCF margin volatility</td><td>{{ pct(score.ocf_margin_volatility) }}</td></tr>
              <tr><td style="padding-top:0.75rem;color:var(--pico-muted-color)">Sector score</td><td>{{ score.sector_score?.toFixed(2) ?? '—' }}</td></tr>
              <tr><td style="color:var(--pico-muted-color)">Geography</td><td>{{ score.geo_score?.toFixed(2) ?? '—' }}</td></tr>
              <tr><td style="color:var(--pico-muted-color)">Debt</td><td>{{ score.debt_score?.toFixed(2) ?? '—' }}</td></tr>
              <tr><td style="color:var(--pico-muted-color)">Trend</td><td>{{ score.trend_score?.toFixed(2) ?? '—' }}</td></tr>
              <tr><td style="color:var(--pico-muted-color)">VD</td><td>{{ score.vd_score?.toFixed(2) ?? '—' }}</td></tr>
            </tbody>
          </table>
          <footer v-if="score" style="font-size:0.75rem;color:var(--pico-muted-color)">
            Scored {{ fmtDate(score.score_computed_at) }}
          </footer>
        </article>

        <!-- Technical snapshot card -->
        <article style="margin:0">
          <header><strong>Technical Snapshot</strong></header>
          <div v-if="!ta" style="color:var(--pico-muted-color);padding:0.5rem 0">No TA data available.</div>
          <table v-else style="margin:0">
            <tbody>
              <tr>
                <td>MA Cross</td>
                <td>
                  <span :class="['badge', maCrossColor(ta.ma_cross_signal)]">
                    {{ ta.ma_cross_signal?.replace('_', ' ') ?? '—' }}
                  </span>
                  <span v-if="ta.ma_cross_days_ago != null" style="margin-left:0.5rem;font-size:0.8rem;color:var(--pico-muted-color)">
                    {{ ta.ma_cross_days_ago }}d ago
                  </span>
                </td>
              </tr>
              <tr>
                <td>MA Distance</td>
                <td :style="{ color: (ta.ma_distance_pct ?? 0) >= 0 ? 'var(--pico-ins-color)' : 'inherit' }">
                  {{ ta.ma_distance_pct != null ? ta.ma_distance_pct.toFixed(2) + '%' : '—' }}
                </td>
              </tr>
              <tr>
                <td>MA200 Trend</td>
                <td><span :class="['badge', trendColor(ta.ma200_trend)]">{{ ta.ma200_trend ?? '—' }}</span></td>
              </tr>
              <tr v-if="ta.ma200_bottom_days_ago != null">
                <td>MA200 Bottom</td>
                <td>{{ ta.ma200_bottom_days_ago }}d ago</td>
              </tr>
              <tr v-if="ta.ma50_bottom_days_ago != null">
                <td>MA50 Bottom</td>
                <td>{{ ta.ma50_bottom_days_ago }}d ago</td>
              </tr>
              <tr><td>RSI (14)</td><td>{{ ta.rsi14?.toFixed(1) ?? '—' }}</td></tr>
              <tr>
                <td>MACD</td>
                <td><span :class="['badge', sentimentColor(ta.macd_sentiment)]">{{ ta.macd_sentiment ?? '—' }}</span></td>
              </tr>
              <tr>
                <td>OBV</td>
                <td><span :class="['badge', trendColor(ta.obv_trend)]">{{ ta.obv_trend ?? '—' }}</span></td>
              </tr>
              <tr><td>Close</td><td>{{ ta.close_price?.toFixed(2) ?? '—' }}</td></tr>
            </tbody>
          </table>
          <footer v-if="ta" style="font-size:0.75rem;color:var(--pico-muted-color)">
            Computed {{ fmtDate(ta.ta_computed_at) }}
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
      <article style="margin:0">
        <header>
          <strong>Price — {{ symbol }}</strong>
          <span style="float:right;font-size:0.75rem;color:var(--pico-muted-color)">
            <span style="color:#ff9800">— MA50</span>&ensp;
            <span style="color:#2196f3">— MA200</span>&ensp;
            <span style="color:#ce93d8">RSI14</span>&ensp;
            <span style="color:#42a5f5">MACD</span>/<span style="color:#ef9a9a">Sig</span>
          </span>
        </header>
        <PriceChart :symbol="symbol" />
      </article>
    </template>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { createChart, LineSeries } from 'lightweight-charts'
import { api } from '../lib/api'
import PriceChart from '../components/PriceChart.vue'

const route  = useRoute()
const router = useRouter()
const symbol = route.params.symbol

const backLabel = computed(() => {
  const from = router.options.history.state?.back ?? ''
  if (typeof from === 'string') {
    if (from.startsWith('/screen'))   return 'Back to Screen'
    if (from.startsWith('/holdings')) return 'Back to Holdings'
    if (from === '/')                 return 'Back to Dashboard'
  }
  return 'Back'
})

const score   = ref(null)
const ta      = ref(null)
const history = ref([])
const loading = ref(true)
const error   = ref(null)
const chartEl = ref(null)
let chart = null

function pct(n) {
  if (n == null) return '—'
  return (n * 100).toFixed(1) + '%'
}
function fmtCap(n) {
  if (n == null) return '—'
  if (n >= 1e12) return (n / 1e12).toFixed(2) + 'T'
  if (n >= 1e9)  return (n / 1e9).toFixed(2) + 'B'
  if (n >= 1e6)  return (n / 1e6).toFixed(2) + 'M'
  return n.toLocaleString()
}
function fmtVol(n) {
  if (n == null) return '—'
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B'
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return n.toLocaleString()
}
function fmtDate(s) {
  if (!s) return '—'
  return new Date(s).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' })
}
function maCrossColor(v) {
  if (v === 'golden_cross') return 'green'
  if (v === 'death_cross')  return 'red'
  return 'grey'
}
function trendColor(v) {
  if (v === 'rising')  return 'green'
  if (v === 'falling') return 'red'
  return 'grey'
}
function sentimentColor(v) {
  if (v === 'bullish') return 'green'
  if (v === 'bearish') return 'red'
  return 'grey'
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

onMounted(async () => {
  const [scoreRes, histRes, taRes] = await Promise.allSettled([
    api.scores.detail(symbol),
    api.scores.history(symbol),
    api.dashboard.taDetail(symbol),
  ])

  if (scoreRes.status === 'fulfilled') {
    score.value = scoreRes.value
  }
  if (histRes.status === 'fulfilled') {
    history.value = histRes.value
  }
  if (taRes.status === 'fulfilled') {
    ta.value = taRes.value
  }

  // Surface a top-level error only if all three failed
  if (scoreRes.status === 'rejected' && taRes.status === 'rejected') {
    error.value = scoreRes.reason?.message ?? 'Failed to load data'
  }

  loading.value = false
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
