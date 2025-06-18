import { writable } from 'svelte/store';

// credential storage
const storedToken = typeof window !== 'undefined' ? localStorage.getItem('jwt') : null;

export const tokenStore = writable(storedToken);

tokenStore.subscribe((value) => {
  if (value) {
    localStorage.setItem('jwt', value);
  } else {
    localStorage.removeItem('jwt');
  }
});