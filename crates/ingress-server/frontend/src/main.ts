import { mount } from 'svelte';
import '@fontsource-variable/red-hat-display';
import '@fontsource-variable/red-hat-mono';
import './app.css';
import App from './App.svelte';
import 'virtual:uno.css';

const app = mount(App, {
  target: document.getElementById('app')!,
});

export default app;
