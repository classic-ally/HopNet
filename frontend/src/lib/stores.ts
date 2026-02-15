import { writable } from 'svelte/store';

// API Configuration - use injected port from Vite build
declare const __BACKEND_PORT__: number;
export const API_BASE_URL = `http://localhost:${__BACKEND_PORT__}`;
export const BACKEND_PORT = __BACKEND_PORT__;

// Current browse path store
export const currentPathStore = writable('/');

// Refresh trigger store - increment to trigger refresh
export const refreshTriggerStore = writable(0);

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

// Helper function to clear authentication and return to login
export async function clearAuth() {
  const token = localStorage.getItem('jwt');
  if (token) {
    try {
      await fetch(`${API_BASE_URL}/logout`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${token}` },
      });
    } catch (e) { /* best-effort */ }
  }
  localStorage.removeItem('jwt');
  tokenStore.set(null);
}

// Authenticated fetch wrapper that handles 401 responses
export async function authenticatedFetch(url: string, options: RequestInit = {}) {
  const token = localStorage.getItem('jwt');

  if (!token) {
    throw new Error('No authentication token found');
  }

  // Add Authorization header
  const headers = new Headers(options.headers || {});
  headers.set('Authorization', `Bearer ${token}`);

  const response = await fetch(url, {
    ...options,
    headers,
  });

  // If we get a 401, the JWT is invalid - clear it and force re-login
  if (response.status === 401) {
    clearAuth();
    throw new Error('Authentication failed - please log in again');
  }

  return response;
}