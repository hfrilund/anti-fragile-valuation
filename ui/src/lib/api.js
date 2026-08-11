const BASE = import.meta.env.VITE_API_BASE ?? ''

function token() {
  return localStorage.getItem('afv_token') ?? ''
}

async function request(path, params = {}) {
  const url = new URL(BASE + path, window.location.origin)
  Object.entries(params).forEach(([k, v]) => v != null && url.searchParams.set(k, v))

  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token()}` },
  })

  if (res.status === 401) throw new Error('unauthorized')
  if (!res.ok) throw new Error(`API error ${res.status}`)
  return res.json()
}

async function put(path, body) {
  const url = new URL(BASE + path, window.location.origin)
  const res = await fetch(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token()}` },
    body: JSON.stringify(body),
  })
  if (res.status === 401) throw new Error('unauthorized')
  if (!res.ok) throw new Error(`API error ${res.status}`)
  return res.json()
}

export const api = {
  dashboard: {
    topPicks:         (params)  => request('/api/dashboard/top-picks', params),
    potentialCrosses: (params)  => request('/api/dashboard/potential-crosses', params),
    earlyRecovery:    (params)  => request('/api/dashboard/early-recovery', params),
    taDetail:         (symbol)  => request(`/api/dashboard/ta/${symbol}`),
    holdings:         ()        => request('/api/dashboard/holdings'),
    sharpe:           (params)  => request('/api/dashboard/sharpe', params).then(r => r.data),
    sharpeHistory:    (params)  => request('/api/dashboard/sharpe/history', params).then(r => r.data),
  },
  holdings: {
    detail:     (symbol)         => request(`/api/holdings/${symbol}`),
    saveThesis: (symbol, thesis) => put(`/api/holdings/${symbol}/thesis`, { thesis }),
  },
  scores: {
    detail:  (symbol) => request(`/api/scores/${symbol}/detail`),
    history: (symbol) => request(`/api/scores/${symbol}/history`),
  },
  prices: {
    history: (symbol) => request(`/api/prices/${symbol}`),
  },
  screen: {
    options: ()       => request('/api/screen/options'),
    run:     (params) => request('/api/screen', params),
  },
}
