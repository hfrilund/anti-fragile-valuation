<template>
  <div>
    <h2>Screen</h2>

    <!-- Filter panel -->
    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:1rem;margin-bottom:1.5rem;align-items:end">

      <label style="margin:0">
        <span style="font-size:0.8rem;color:var(--pico-muted-color)">Sector</span>
        <select v-model="filters.sector" @change="filters.industry = null" style="margin-top:0.25rem">
          <option :value="null">All sectors</option>
          <option v-for="s in options.sectors" :key="s" :value="s">{{ s }}</option>
        </select>
      </label>

      <label style="margin:0">
        <span style="font-size:0.8rem;color:var(--pico-muted-color)">Industry</span>
        <select v-model="filters.industry" :disabled="!filters.sector" style="margin-top:0.25rem">
          <option :value="null">All industries</option>
          <option v-for="i in industriesForSector" :key="i" :value="i">{{ i }}</option>
        </select>
      </label>

      <label style="margin:0">
        <span style="font-size:0.8rem;color:var(--pico-muted-color)">Cap size</span>
        <select v-model="filters.cap_size" style="margin-top:0.25rem">
          <option :value="null">Any</option>
          <option value="micro">Micro (&lt;$300M)</option>
          <option value="small">Small ($300M–$2B)</option>
          <option value="mid">Mid ($2B–$10B)</option>
          <option value="large">Large ($10B–$200B)</option>
          <option value="mega">Mega (&gt;$200B)</option>
        </select>
      </label>

      <label style="margin:0">
        <span style="font-size:0.8rem;color:var(--pico-muted-color)">Min AFV21</span>
        <input type="number" v-model.number="filters.min_afv21" step="0.1" style="margin-top:0.25rem" />
      </label>

      <label style="margin:0">
        <span style="font-size:0.8rem;color:var(--pico-muted-color)">MA cross</span>
        <select v-model="filters.ma_cross" style="margin-top:0.25rem">
          <option :value="null">Any</option>
          <option value="golden_cross">Golden cross</option>
          <option value="death_cross">Death cross</option>
          <option value="potential_cross">Potential cross</option>
        </select>
      </label>

      <label style="margin:0">
        <span style="font-size:0.8rem;color:var(--pico-muted-color)">MACD</span>
        <select v-model="filters.macd" style="margin-top:0.25rem">
          <option :value="null">Any</option>
          <option value="bullish">Bullish</option>
          <option value="neutral">Neutral</option>
          <option value="bearish">Bearish</option>
        </select>
      </label>

      <div style="display:flex;gap:0.5rem">
        <label style="margin:0;flex:1">
          <span style="font-size:0.8rem;color:var(--pico-muted-color)">RSI min</span>
          <input type="number" v-model.number="filters.min_rsi" min="0" max="100" step="1" placeholder="0" style="margin-top:0.25rem" />
        </label>
        <label style="margin:0;flex:1">
          <span style="font-size:0.8rem;color:var(--pico-muted-color)">RSI max</span>
          <input type="number" v-model.number="filters.max_rsi" min="0" max="100" step="1" placeholder="100" style="margin-top:0.25rem" />
        </label>
      </div>

      <div style="display:flex;flex-direction:column;gap:0.35rem">
        <span style="font-size:0.8rem;color:var(--pico-muted-color)">Results</span>
        <select v-model.number="filters.limit" style="margin-top:0.25rem">
          <option :value="50">Top 50</option>
          <option :value="100">Top 100</option>
          <option :value="250">Top 250</option>
          <option :value="500">Top 500</option>
        </select>
      </div>

      <button @click="run" :aria-busy="loading" style="align-self:end;margin:0">
        Screen
      </button>
    </div>

    <!-- Results -->
    <div v-if="error" style="color:var(--pico-del-color)">{{ error }}</div>

    <div v-else-if="!ran" style="color:var(--pico-muted-color)">
      Set your filters and click Screen.
    </div>

    <template v-else>
      <p style="color:var(--pico-muted-color);font-size:0.85rem;margin-bottom:0.75rem">
        {{ results.length }} results
      </p>

      <div v-if="!results.length" style="color:var(--pico-muted-color)">No results match your filters.</div>

      <div v-else class="overflow-auto">
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Name</th>
              <th>Sector</th>
              <th>Industry</th>
              <th>Cap</th>
              <th>Avg Vol</th>
              <th>AFV21</th>
              <th>RP21</th>
              <th>MA Cross</th>
              <th>Distance</th>
              <th>RSI</th>
              <th>MACD</th>
              <th>OBV</th>
              <th>MA200</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="r in results"
              :key="r.symbol"
              style="cursor:pointer"
              @click="$router.push(`/ta/${r.symbol}`)"
            >
              <td><strong>{{ r.symbol }}</strong></td>
              <td>{{ r.asset_name ?? '—' }}</td>
              <td>{{ r.sector ?? '—' }}</td>
              <td>{{ r.industry ?? '—' }}</td>
              <td>{{ fmtCap(r.market_cap) }}</td>
              <td>{{ fmtVol(r.avg_volume_3m) }}</td>
              <td><strong>{{ r.afv21?.toFixed(2) ?? '—' }}</strong></td>
              <td>{{ r.rp21?.toFixed(2) ?? '—' }}</td>
              <td>
                <span v-if="r.ma_cross_signal" :class="['badge', maCrossColor(r.ma_cross_signal)]">
                  {{ r.ma_cross_signal.replace('_', ' ') }}
                </span>
                <span v-else style="color:var(--pico-muted-color)">—</span>
              </td>
              <td :style="{ color: r.ma_distance_pct != null && r.ma_distance_pct >= 0 ? 'var(--pico-ins-color)' : 'inherit' }">
                {{ r.ma_distance_pct != null ? r.ma_distance_pct.toFixed(2) + '%' : '—' }}
              </td>
              <td>{{ r.rsi14?.toFixed(1) ?? '—' }}</td>
              <td>
                <span v-if="r.macd_sentiment" :class="['badge', sentimentColor(r.macd_sentiment)]">
                  {{ r.macd_sentiment }}
                </span>
                <span v-else style="color:var(--pico-muted-color)">—</span>
              </td>
              <td>
                <span v-if="r.obv_trend" :class="['badge', trendColor(r.obv_trend)]">{{ r.obv_trend }}</span>
                <span v-else style="color:var(--pico-muted-color)">—</span>
              </td>
              <td>
                <span v-if="r.ma200_trend" :class="['badge', trendColor(r.ma200_trend)]">{{ r.ma200_trend }}</span>
                <span v-else style="color:var(--pico-muted-color)">—</span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>
  </div>
</template>

<script>
export default { name: 'Screen' }
</script>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { api } from '../lib/api'

const options  = ref({ sectors: [], industries_by_sector: {} })
const results  = ref([])
const loading  = ref(false)
const error    = ref(null)
const ran      = ref(false)

const filters = ref({
  sector:    null,
  industry:  null,
  cap_size:  null,
  min_afv21: 0,
  ma_cross:  null,
  macd:      null,
  min_rsi:   null,
  max_rsi:   null,
  limit:     100,
})

const industriesForSector = computed(() => {
  if (!filters.value.sector) return []
  return options.value.industries_by_sector[filters.value.sector] ?? []
})

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

async function run() {
  loading.value = true
  error.value   = null
  ran.value     = true
  try {
    const params = {}
    const f = filters.value
    if (f.sector)              params.sector    = f.sector
    if (f.industry)            params.industry  = f.industry
    if (f.cap_size)            params.cap_size  = f.cap_size
    if (f.min_afv21 !== 0)    params.min_afv21 = f.min_afv21
    if (f.ma_cross)            params.ma_cross  = f.ma_cross
    if (f.macd)                params.macd      = f.macd
    if (f.min_rsi != null)     params.min_rsi   = f.min_rsi
    if (f.max_rsi != null)     params.max_rsi   = f.max_rsi
    params.limit = f.limit

    const data = await api.screen.run(params)
    results.value = data.results
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

onMounted(async () => {
  try {
    options.value = await api.screen.options()
  } catch (e) {
    // non-fatal — dropdowns just stay empty
  }
})
</script>
