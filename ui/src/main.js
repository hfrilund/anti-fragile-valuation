import { createApp } from 'vue'
import '@picocss/pico/css/pico.min.css'
import './app.css'
import App from './App.vue'
import router from './router'

createApp(App).use(router).mount('#app')
