import { mount } from 'svelte'
// Must come before app.css so the app's own declarations win.
//
// preset-mini emits border utilities as width only (`.border-b` is
// `border-bottom-width: 1px`), and CSS computes a border width of 0 whenever
// border-style is `none` — the initial value. Without a reset establishing
// `border-style: solid`, every such utility renders nothing, which is why 23
// files had taken to writing `border-solid` by hand while ~70 other call sites
// silently drew no border at all. Tailwind's preflight does this; UnoCSS leaves
// it to the app. The `-compat` variant is the retrofit-safe one: it omits the
// `background-color: transparent` on buttons that breaks existing components.
import '@unocss/reset/tailwind-compat.css'
import '@fontsource-variable/red-hat-display'
import '@fontsource-variable/red-hat-mono'
import './app.css'
import App from './App.svelte'
import 'virtual:uno.css'

const app = mount(App, {
  target: document.getElementById('app')!,
})

export default app
