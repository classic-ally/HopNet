// Thin REST client for the ingress-server API. The session lives in an httpOnly
// cookie set by the BFF, so requests just need `credentials: same-origin` and a
// 401 means "not logged in" -> bounce to the server-side OIDC login.
//
// This is the seam that maps onto HopNet's file API at fold-in; keep it small.

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

/** Redirect the browser into the server-side OIDC login flow. */
export function login(): void {
  window.location.href = '/auth/login';
}

// A 401 anywhere (boot probe or a mid-session expiry during grid fetches)
// flips the app to the login page rather than hard-redirecting into the OIDC
// provider — the user gets a deliberate "Sign in" step, not a surprise bounce.
let onUnauthorized: (() => void) | null = null;
export function setUnauthorizedHandler(fn: () => void): void {
  onUnauthorized = fn;
}

async function request(path: string, init?: RequestInit): Promise<Response> {
  const res = await fetch(path, { credentials: 'same-origin', ...init });
  if (res.status === 401) {
    onUnauthorized?.();
    throw new ApiError(401, 'Not signed in');
  }
  return res;
}

/** GET a JSON API route. Throws ApiError on non-2xx (except 401, which redirects). */
export async function apiJson<T>(path: string): Promise<T> {
  const res = await request(path);
  if (!res.ok) throw new ApiError(res.status, `${res.status} ${res.statusText}`);
  return (await res.json()) as T;
}

/** Absolute URL for an image/blob route, for use as an `src`. */
export function assetUrl(path: string): string {
  return path;
}
