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

async function request(path: string, init?: RequestInit): Promise<Response> {
  const res = await fetch(path, { credentials: 'same-origin', ...init });
  if (res.status === 401) {
    login();
    // Give the redirect a tick; never resolve so callers don't render on a 401.
    await new Promise(() => {});
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
