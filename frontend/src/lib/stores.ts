import { writable } from 'svelte/store';

// Helper to parse a JWT and get its expiration time
function getJwtExpiration(token: string) {
  try {
    const base64Url = token.split('.')[1];
    const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
    const jsonPayload = decodeURIComponent(
      atob(base64).split('').map(function (c) {
        return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
      }).join('')
    );
    const payload = JSON.parse(jsonPayload);
    return payload.exp; // Expiry time in seconds
  } catch (e) {
    console.error('Invalid JWT', e);
    return null;
  }
}

// credential storage
const storedToken = typeof window !== 'undefined' ? localStorage.getItem('jwt') : null;

// Check if token is valid
let isValidToken = false;
if (storedToken) {
  const exp = getJwtExpiration(storedToken);
  if (exp && Date.now() < exp * 1000) {
    isValidToken = true;
  }
}

export const tokenStore = writable(isValidToken ? storedToken : null);

tokenStore.subscribe((value) => {
  if (value) {
    const exp = getJwtExpiration(value);
    if (exp && Date.now() < exp * 1000) {
      localStorage.setItem('jwt', value);
    } else {
      // Remove if expired
      localStorage.removeItem('jwt');
      tokenStore.set(null); // Also clear the store
    }
  } else {
    localStorage.removeItem('jwt');
  }
});