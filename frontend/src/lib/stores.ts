import { writable } from 'svelte/store';

// API base is always relative — the webview (Tauri or browser) is served
// from the same axum origin in every mode, so `fetch('/api/...')` resolves
// against whichever port the server bound (fixed in headless, kernel-assigned
// in GUI). Vite dev proxies API routes back to the headless backend.
export const API_BASE_URL = '';

// Current browse path store
export const currentPathStore = writable('/');

// Refresh trigger store - increment to trigger refresh
export const refreshTriggerStore = writable(0);

// Parse JWT payload
function parseJwtPayload(token: string): any | null {
  try {
    const base64Url = token.split('.')[1];
    const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
    const jsonPayload = decodeURIComponent(
      atob(base64).split('').map(function (c) {
        return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
      }).join('')
    );
    return JSON.parse(jsonPayload);
  } catch (e) {
    return null;
  }
}

// Get the current user ID from the stored JWT
export function getCurrentUserId(): number | null {
  const token = typeof window !== 'undefined' ? localStorage.getItem('jwt') : null;
  if (!token) return null;
  const payload = parseJwtPayload(token);
  if (!payload?.uid) return null;
  const id = parseInt(payload.uid, 10);
  return isNaN(id) ? null : id;
}

// Helper to parse a JWT and get its expiration time
function getJwtExpiration(token: string) {
  const payload = parseJwtPayload(token);
  return payload?.exp ?? null;
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

// Current user profile store — fetched on login, cleared on logout
import type { SelfUserInfo } from './types';
export const currentUserStore = writable<SelfUserInfo | null>(null);

export async function refreshCurrentUser() {
  try {
    const response = await authenticatedFetch(`${API_BASE_URL}/users/me`);
    if (response.ok) {
      currentUserStore.set(await response.json());
    }
  } catch (_) { /* ignore */ }
}

// Incoming share count store — polled every 30s when authenticated
export const incomingShareCountStore = writable(0);

let shareCountInterval: ReturnType<typeof setInterval> | null = null;

async function pollShareCount() {
  try {
    const response = await authenticatedFetch(`${API_BASE_URL}/shares/incoming/count`);
    if (response.ok) {
      const data = await response.json();
      incomingShareCountStore.set(data.count);
    }
  } catch (_) { /* ignore — not logged in or network error */ }
}

// =====================================================================
// Import status store — 1 Hz poll while subscribed AND status is non-terminal.
// Drives onboarding banner, write-affordance gating, ImportPane progress UI.
// Defined BEFORE the tokenStore.subscribe block below so its module-init
// invocation (when a stored JWT is already present) doesn't TDZ on the
// importStatusStore reference inside that callback.
// =====================================================================

import type { ImportRecord, ImportPathCounts, ImportStatus } from './types';
import { fetchCurrentImport, fetchImportCounts } from './api/import';

export interface ImportStatusState {
  record: ImportRecord | null;
  counts: ImportPathCounts | null;  // null on non-owner or no active import
  loading: boolean;
}

const IMPORT_POLL_MS = 1000;

function isTerminal(status: ImportStatus | undefined): boolean {
  return status === 'Completed' || status === 'Failed';
}

function createImportStatusStore() {
  const inner = writable<ImportStatusState>({ record: null, counts: null, loading: false });
  let interval: ReturnType<typeof setInterval> | null = null;
  let subscriberCount = 0;

  async function tick() {
    inner.update(s => ({ ...s, loading: true }));
    try {
      const record = await fetchCurrentImport();
      const counts = (record && !isTerminal(record.status)) ? await fetchImportCounts() : null;
      inner.set({ record, counts, loading: false });
      // Stop polling if no active import or terminal — single final fetch already done.
      if (!record || isTerminal(record.status)) {
        stopPolling();
      }
    } catch (_) {
      inner.update(s => ({ ...s, loading: false }));
    }
  }

  function startPolling() {
    if (interval) return;
    interval = setInterval(tick, IMPORT_POLL_MS);
  }

  function stopPolling() {
    if (interval) {
      clearInterval(interval);
      interval = null;
    }
  }

  return {
    subscribe(run: (value: ImportStatusState) => void, invalidate?: any) {
      subscriberCount++;
      // Kick a fetch on first subscribe so consumers see fresh state immediately.
      if (subscriberCount === 1) {
        tick();
        startPolling();
      }
      const unsub = inner.subscribe(run, invalidate);
      return () => {
        subscriberCount--;
        if (subscriberCount === 0) stopPolling();
        unsub();
      };
    },
    /// Manual kick — call after upload submit or onboarding flag change to
    /// pull fresh state without waiting for the next interval tick.
    refresh: tick,
    /// Reset to empty + stop polling. Called on logout.
    reset() {
      stopPolling();
      subscriberCount = 0;
      inner.set({ record: null, counts: null, loading: false });
    },
  };
}

export const importStatusStore = createImportStatusStore();

import { derived, type Readable } from 'svelte/store';

/// True while a non-terminal import is owned by this user. Frontend uses this
/// to disable write affordances (uploads, share grants, deletes, …) — the
/// security boundary is the backend `import_gate` middleware that 409s the
/// same routes; this store is purely UX polish.
export const writesGatedStore: Readable<boolean> = derived(
    importStatusStore,
    ($s) => $s.record?.status === 'Importing' || $s.record?.status === 'Pending',
);

export const WRITES_GATED_TOOLTIP = 'Disabled while your data import is in progress.';


// Token-driven session lifecycle. Runs once at module init for any stored
// JWT, then on every subsequent token change. Defined here (after
// importStatusStore) so the cleanup branch can call importStatusStore.reset()
// without hitting the TDZ.
tokenStore.subscribe((token) => {
  if (shareCountInterval) {
    clearInterval(shareCountInterval);
    shareCountInterval = null;
  }
  if (token) {
    refreshCurrentUser();
    pollShareCount();
    shareCountInterval = setInterval(pollShareCount, 30_000);
  } else {
    currentUserStore.set(null);
    incomingShareCountStore.set(0);
    importStatusStore.reset();
  }
});
