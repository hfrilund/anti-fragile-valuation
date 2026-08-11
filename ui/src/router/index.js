import { createRouter, createWebHistory } from 'vue-router'
import Dashboard from '../views/Dashboard.vue'

const routes = [
  { path: '/',                        component: Dashboard },
  { path: '/scores',                  component: () => import('../views/Scores.vue') },
  { path: '/screen',                  component: () => import('../views/Screen.vue') },
  { path: '/scores/:symbol/history',  component: () => import('../views/ScoreHistory.vue') },
  { path: '/ta/:symbol',              component: () => import('../views/TechnicalAnalysis.vue') },
  { path: '/holdings/:symbol',        component: () => import('../views/HoldingDetail.vue') },
  { path: '/portfolio',               component: () => import('../views/Portfolio.vue') },
]

export default createRouter({
  history: createWebHistory(),
  routes,
})
