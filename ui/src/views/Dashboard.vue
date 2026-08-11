<template>
  <div>
    <h2>Dashboard</h2>
    <p v-if="runDate" style="color:var(--pico-muted-color);margin-top:-0.75rem;margin-bottom:1.5rem">
      Last run: {{ runDate }}
    </p>

    <!-- Portfolio MTM summary -->
    <div v-if="mtmSummary.length" style="margin-bottom:2rem">
      <!-- Hero: total EUR value -->
      <div class="mtm-hero">
        <div class="mtm-hero-label">Total portfolio value</div>
        <div class="mtm-hero-value">
          {{ totalEur != null ? '€' + fmtLarge(totalEur) : '—' }}
        </div>
        <div v-if="totalPnlEur != null" class="mtm-hero-pnl" :class="totalPnlEur >= 0 ? 'pnl-pos' : 'pnl-neg'">
          {{ totalPnlEur >= 0 ? '+' : '' }}€{{ fmtLarge(totalPnlEur) }}
          ({{ totalPnlPct != null ? (totalPnlPct >= 0 ? '+' : '') + totalPnlPct.toFixed(1) + '%' : '' }})
          <span style="font-size:0.7rem;opacity:0.7;margin-left:0.3rem">vs cost basis</span>
        </div>
      </div>

      <!-- Per-currency breakdown -->
      <div class="stats-row" style="margin-top:1rem">
        <div class="stat-card" v-for="g in mtmSummary" :key="g.currency">
          <div class="stat-label">{{ g.currency }}</div>
          <div class="stat-value">{{ fmtLarge(g.value) }}</div>
          <div class="stat-sub" :class="g.pnl >= 0 ? 'pnl-pos' : 'pnl-neg'">
            {{ g.pnl >= 0 ? '+' : '' }}{{ fmtLarge(g.pnl) }} ({{ g.pnl_pct.toFixed(1) }}%)
          </div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Positions</div>
          <div class="stat-value">{{ holdings.length }}</div>
          <div class="stat-sub">holdings</div>
        </div>
      </div>
    </div>

    <section>
      <h3>Holdings</h3>

      <!-- Portfolio stats row -->
      <div v-if="sharpe" class="stats-row">
        <div class="stat-card">
          <div class="stat-label">Sharpe ratio</div>
          <div class="stat-value" :class="sharpeColor(sharpe.sharpe)">{{ sharpe.sharpe?.toFixed(2) ?? 'N/A' }}</div>
          <div class="stat-sub">{{ sharpe.lookback_days }}d lookback</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Ann. return</div>
          <div class="stat-value" :class="sharpe.ann_return >= 0 ? 'pos' : 'neg'">{{ pct(sharpe.ann_return) }}</div>
          <div class="stat-sub">{{ sharpe.period_start }} → {{ sharpe.period_end }}</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Volatility</div>
          <div class="stat-value">{{ pct(sharpe.ann_vol) }}</div>
          <div class="stat-sub">annualised</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">Max drawdown</div>
          <div class="stat-value neg">{{ pct(sharpe.max_drawdown) }}</div>
          <div class="stat-sub">{{ sharpe.trading_days }} trading days</div>
        </div>
      </div>
      <div v-else-if="sharpeLoading" style="margin-bottom:1rem;color:var(--pico-muted-color);font-size:0.85rem">Loading portfolio stats…</div>
      <p v-else-if="!sharpeLoading" style="color:var(--pico-muted-color);font-size:0.85rem;margin-bottom:1rem">Portfolio stats unavailable — save holdings and fetch price history first.</p>

      <p v-if="holdingsError" style="color:var(--pico-del-color)">{{ holdingsError }}</p>
      <div v-else-if="!holdings.length" aria-busy="true">Loading…</div>
      <div v-else class="overflow-auto">
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Name</th>
              <th>Position</th>
              <th>Value</th>
              <th>P&L</th>
              <th>AFV21</th>
              <th>MA Cross</th>
              <th>MA200</th>
              <th>RSI14</th>
              <th>MACD</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="h in holdings" :key="h.symbol" style="cursor:pointer" @click="$router.push(`/holdings/${h.symbol}`)">
              <td><strong>{{ h.symbol }}</strong></td>
              <td>{{ h.description }}</td>
              <td>{{ h.position }}</td>
              <td>{{ h.currency }} {{ fmt(h.pos_value) }}</td>
              <td :class="h.pnl_pct >= 0 ? 'pnl-pos' : 'pnl-neg'">{{ h.pnl_pct?.toFixed(1) }}%</td>
              <td>{{ h.afv21?.toFixed(2) }}</td>
              <td><span :class="['badge', maCrossColor(h.ma_cross_signal)]">{{ h.ma_cross_signal?.replace('_', ' ') }}</span></td>
              <td><span :class="['badge', trendColor(h.ma200_trend)]">{{ h.ma200_trend }}</span></td>
              <td>{{ h.rsi14?.toFixed(1) }}</td>
              <td><span :class="['badge', sentimentColor(h.macd_sentiment)]">{{ h.macd_sentiment }}</span></td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section style="margin-top:2rem">
      <div style="display:flex;align-items:center;gap:1rem;margin-bottom:0.75rem">
        <h3 style="margin:0">Top Picks</h3>
        <label style="display:flex;align-items:center;gap:0.4rem;margin:0;font-size:0.85rem">
          <input type="checkbox" v-model="goldenOnly" role="switch" style="margin:0" />
          Golden cross only
        </label>
        <RouterLink to="/scores" style="margin-left:auto;font-size:0.85rem">Browse all →</RouterLink>
      </div>
      <p v-if="picksError" style="color:var(--pico-del-color)">{{ picksError }}</p>
      <div v-else-if="!picks.length" aria-busy="true">Loading…</div>
      <div v-else class="overflow-auto">
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Name</th>
              <th>AFV21</th>
              <th>MA Cross</th>
              <th>MA200</th>
              <th>RSI14</th>
              <th>MACD</th>
              <th>OBV</th>
              <th>Price</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="p in picks" :key="p.symbol" style="cursor:pointer" @click="$router.push(`/ta/${p.symbol}`)">
              <td><strong>{{ p.symbol }}</strong></td>
              <td>{{ p.asset_name }}</td>
              <td><strong>{{ p.afv21?.toFixed(2) }}</strong></td>
              <td><span :class="['badge', maCrossColor(p.ma_cross_signal)]">{{ p.ma_cross_signal?.replace('_', ' ') }}</span></td>
              <td><span :class="['badge', trendColor(p.ma200_trend)]">{{ p.ma200_trend }}</span></td>
              <td>{{ p.rsi14?.toFixed(1) }}</td>
              <td><span :class="['badge', sentimentColor(p.macd_sentiment)]">{{ p.macd_sentiment }}</span></td>
              <td><span :class="['badge', trendColor(p.obv_trend)]">{{ p.obv_trend }}</span></td>
              <td>{{ p.close_price?.toFixed(2) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section style="margin-top:2rem">
      <h3>Potential Crosses</h3>
      <p style="color:var(--pico-muted-color);margin-top:-0.5rem;margin-bottom:0.75rem;font-size:0.85rem">
        Death cross stocks where 50MA is within 3% of 200MA — ranked closest to crossing first
      </p>
      <p v-if="crossesError" style="color:var(--pico-del-color)">{{ crossesError }}</p>
      <div v-else-if="crossesLoading" aria-busy="true">Loading…</div>
      <p v-else-if="!potentialCrosses.length" style="color:var(--pico-muted-color)">No potential crosses found.</p>
      <div v-else class="overflow-auto">
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Name</th>
              <th>AFV21</th>
              <th>Distance</th>
              <th>MA200</th>
              <th>RSI14</th>
              <th>MACD</th>
              <th>OBV</th>
              <th>Price</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="p in potentialCrosses" :key="p.symbol" style="cursor:pointer" @click="$router.push(`/ta/${p.symbol}`)">
              <td><strong>{{ p.symbol }}</strong></td>
              <td>{{ p.asset_name }}</td>
              <td><strong>{{ p.afv21?.toFixed(2) }}</strong></td>
              <td :style="{ color: p.ma_distance_pct >= 0 ? 'var(--pico-ins-color)' : 'inherit' }">
                {{ p.ma_distance_pct?.toFixed(2) }}%
              </td>
              <td><span :class="['badge', trendColor(p.ma200_trend)]">{{ p.ma200_trend }}</span></td>
              <td>{{ p.rsi14?.toFixed(1) }}</td>
              <td><span :class="['badge', sentimentColor(p.macd_sentiment)]">{{ p.macd_sentiment }}</span></td>
              <td><span :class="['badge', trendColor(p.obv_trend)]">{{ p.obv_trend }}</span></td>
              <td>{{ p.close_price?.toFixed(2) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section style="margin-top:2rem">
      <h3>Early Recovery</h3>
      <p style="color:var(--pico-muted-color);margin-top:-0.5rem;margin-bottom:0.75rem;font-size:0.85rem">
        50MA recently bottomed with 200MA rising or also bottomed — ranked by AFV21
      </p>
      <p v-if="recoveryError" style="color:var(--pico-del-color)">{{ recoveryError }}</p>
      <div v-else-if="recoveryLoading" aria-busy="true">Loading…</div>
      <p v-else-if="!earlyRecovery.length" style="color:var(--pico-muted-color)">No early recovery signals found.</p>
      <div v-else class="overflow-auto">
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Name</th>
              <th>AFV21</th>
              <th>MA Cross</th>
              <th>50MA Bottom</th>
              <th>200MA</th>
              <th>RSI14</th>
              <th>MACD</th>
              <th>Price</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="p in earlyRecovery" :key="p.symbol" style="cursor:pointer" @click="$router.push(`/ta/${p.symbol}`)">
              <td><strong>{{ p.symbol }}</strong></td>
              <td>{{ p.asset_name }}</td>
              <td><strong>{{ p.afv21?.toFixed(2) }}</strong></td>
              <td><span :class="['badge', maCrossColor(p.ma_cross_signal)]">{{ p.ma_cross_signal?.replace('_', ' ') }}</span></td>
              <td>{{ p.ma50_bottom_days_ago }}d ago</td>
              <td>
                <span v-if="p.ma200_bottom_days_ago != null" class="badge green">bottomed {{ p.ma200_bottom_days_ago }}d ago</span>
                <span v-else :class="['badge', trendColor(p.ma200_trend)]">{{ p.ma200_trend }}</span>
              </td>
              <td>{{ p.rsi14?.toFixed(1) }}</td>
              <td><span :class="['badge', sentimentColor(p.macd_sentiment)]">{{ p.macd_sentiment }}</span></td>
              <td>{{ p.close_price?.toFixed(2) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref, watch, onMounted, computed } from 'vue'
import { RouterLink } from 'vue-router'
import { api } from '../lib/api'

const holdings = ref([])
const holdingsError = ref(null)
const picks = ref([])
const picksError = ref(null)
const goldenOnly = ref(false)
const runDate = ref(null)

const sharpe = ref(null)
const sharpeLoading = ref(true)
const fxRates = ref({})  // { EUR: 1, USD: 1.08, SEK: 11.2, ... } (EUR per 1 CCY unit)

const potentialCrosses = ref([])
const crossesError = ref(null)
const crossesLoading = ref(true)

const earlyRecovery = ref([])
const recoveryError = ref(null)
const recoveryLoading = ref(true)

const mtmSummary = computed(() => {
  if (!holdings.value.length) return []
  const byCcy = {}
  for (const h of holdings.value) {
    const c = h.currency || '?'
    if (!byCcy[c]) byCcy[c] = { currency: c, value: 0, cost: 0 }
    byCcy[c].value += h.pos_value  || 0
    byCcy[c].cost  += h.cost_basis || 0
  }
  return Object.values(byCcy)
    .map(g => ({ ...g, pnl: g.value - g.cost, pnl_pct: g.cost > 0 ? (g.value - g.cost) / g.cost * 100 : 0 }))
    .sort((a, b) => b.value - a.value)
})

// Convert CCY amount to EUR using EURCCY=X rates (rate = how many CCY per 1 EUR)
function toEur(amount, ccy) {
  if (ccy === 'EUR') return amount
  const rate = fxRates.value[ccy]
  return rate ? amount / rate : null
}

const totalEur = computed(() => {
  if (!mtmSummary.value.length) return null
  let total = 0
  for (const g of mtmSummary.value) {
    const eur = toEur(g.value, g.currency)
    if (eur == null) return null
    total += eur
  }
  return total
})

const totalPnlEur = computed(() => {
  if (!mtmSummary.value.length) return null
  let pnl = 0
  for (const g of mtmSummary.value) {
    const eur = toEur(g.pnl, g.currency)
    if (eur == null) return null
    pnl += eur
  }
  return pnl
})

const totalPnlPct = computed(() => {
  if (totalPnlEur.value == null || totalEur.value == null) return null
  const cost = totalEur.value - totalPnlEur.value
  return cost > 0 ? (totalPnlEur.value / cost) * 100 : null
})

function fmt(n) {
  return n?.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })
}
function fmtLarge(n) {
  if (n == null) return '—'
  const abs = Math.abs(n)
  const sign = n < 0 ? '-' : ''
  if (abs >= 1_000_000) return sign + (abs / 1_000_000).toFixed(2) + 'M'
  if (abs >= 1_000)     return sign + (abs / 1_000).toFixed(1) + 'K'
  return sign + abs.toFixed(0)
}
function pct(n) {
  if (n == null) return 'N/A'
  return (n >= 0 ? '+' : '') + (n * 100).toFixed(1) + '%'
}
function sharpeColor(v) {
  if (v == null) return ''
  if (v >= 1.0) return 'pos'
  if (v < 0)    return 'neg'
  return 'neutral'
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

async function loadPicks() {
  picksError.value = null
  picks.value = []
  try {
    const params = { limit: 50 }
    if (goldenOnly.value) params.ma_cross = 'golden_cross'
    const data = await api.dashboard.topPicks(params)
    picks.value = data.results
    if (data.results.length) {
      const d = new Date(data.results[0].ta_computed_at)
      runDate.value = d.toLocaleDateString(undefined, { weekday: 'short', year: 'numeric', month: 'short', day: 'numeric' })
    }
  } catch (e) {
    picksError.value = e.message
  }
}

async function loadPotentialCrosses() {
  crossesError.value = null
  crossesLoading.value = true
  try {
    const data = await api.dashboard.potentialCrosses({ limit: 50 })
    potentialCrosses.value = data.results
  } catch (e) {
    crossesError.value = e.message
  } finally {
    crossesLoading.value = false
  }
}

async function loadEarlyRecovery() {
  recoveryError.value = null
  recoveryLoading.value = true
  try {
    const data = await api.dashboard.earlyRecovery({ limit: 50 })
    earlyRecovery.value = data.results
  } catch (e) {
    recoveryError.value = e.message
  } finally {
    recoveryLoading.value = false
  }
}

onMounted(async () => {
  try {
    const data = await api.dashboard.holdings()
    holdings.value = data.results
    const ccys = [...new Set(data.results.map(h => h.currency).filter(Boolean))]
    if (ccys.length) {
      fxRates.value = await api.dashboard.fxRates(ccys)
    }
  } catch (e) {
    holdingsError.value = e.message
  }

  sharpeLoading.value = true
  try {
    sharpe.value = await api.dashboard.sharpe()
  } catch (e) {
    sharpe.value = null
  } finally {
    sharpeLoading.value = false
  }

  loadPicks()
  loadPotentialCrosses()
  loadEarlyRecovery()
})

watch(goldenOnly, loadPicks)
</script>
