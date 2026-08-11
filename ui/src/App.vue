<template>
  <div v-if="needsToken" style="max-width:400px;margin:10vh auto;padding:1rem">
    <h2>AFV</h2>
    <p>Enter your API token to continue.</p>
    <input v-model="tokenInput" type="password" placeholder="API token" @keyup.enter="saveToken" />
    <button @click="saveToken" style="margin-top:0.5rem;width:100%">Connect</button>
    <p v-if="tokenError" style="color:var(--pico-del-color)">Invalid token.</p>
  </div>

  <div v-else>
    <header class="container-fluid" style="padding:0.75rem 1rem;border-bottom:1px solid var(--pico-muted-border-color)">
      <nav>
        <RouterLink to="/">Dashboard</RouterLink>
        <RouterLink to="/portfolio">Portfolio</RouterLink>
        <RouterLink to="/scores">Scores</RouterLink>
        <RouterLink to="/screen">Screen</RouterLink>
      </nav>
    </header>
    <main class="container-fluid" style="padding:1.5rem 1rem">
      <RouterView v-slot="{ Component }">
        <KeepAlive include="Screen">
          <component :is="Component" />
        </KeepAlive>
      </RouterView>
    </main>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { RouterLink, RouterView } from 'vue-router'
import { api } from './lib/api'

const needsToken = ref(false)
const tokenInput = ref('')
const tokenError = ref(false)

onMounted(async () => {
  if (!localStorage.getItem('afv_token')) {
    needsToken.value = true
    return
  }
  try {
    await api.dashboard.holdings()
  } catch (e) {
    if (e.message === 'unauthorized') {
      localStorage.removeItem('afv_token')
      needsToken.value = true
    }
  }
})

function saveToken() {
  tokenError.value = false
  localStorage.setItem('afv_token', tokenInput.value)
  api.dashboard.holdings()
    .then(() => { needsToken.value = false })
    .catch(() => {
      localStorage.removeItem('afv_token')
      tokenError.value = true
    })
}
</script>
