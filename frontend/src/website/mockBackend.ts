// In-page fake of the node's HTTP API for the website screenshot stories.
//
// Every pane talks to the node through global `fetch('/api/...')` with a JWT
// from `tokenStore`, so swapping `window.fetch` and setting a fake token is
// enough to render the real Interface with curated data — no MSW, no backend.
// `installMockBackend` returns a restore function; stories must call it on
// teardown so one story's routes never leak into the next under vitest.

import { tokenStore } from '../lib/stores';

export type MockResult = Response | unknown;

export interface MockRoute {
    method?: string;
    /** Matched against the request pathname. */
    path: string | RegExp;
    respond: (url: URL, init: RequestInit | undefined, match: RegExpMatchArray | null) => MockResult | Promise<MockResult>;
}

// exp far in the future so the token survives `stores.ts` expiry checks even
// under a frozen clock. Signature is irrelevant: nothing verifies it client-side.
function fakeJwt(uid: number): string {
    const encode = (o: object) => btoa(JSON.stringify(o)).replace(/=+$/, '').replace(/\+/g, '-').replace(/\//g, '_');
    return `${encode({ alg: 'none', typ: 'JWT' })}.${encode({ uid: String(uid), exp: 4102444800 })}.website`;
}

function toResponse(result: MockResult): Response {
    if (result instanceof Response) return result;
    return new Response(JSON.stringify(result), { status: 200, headers: { 'Content-Type': 'application/json' } });
}

export function notFound(): Response {
    return new Response(JSON.stringify({ error: 'not found' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
}

export function installMockBackend(routes: MockRoute[], uid: number): () => void {
    const realFetch = window.fetch.bind(window);

    window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
        const raw = input instanceof Request ? input.url : String(input);
        const url = new URL(raw, window.location.origin);
        if (!url.pathname.startsWith('/api/')) return realFetch(input, init);

        const method = (init?.method ?? (input instanceof Request ? input.method : 'GET')).toUpperCase();
        for (const route of routes) {
            if ((route.method ?? 'GET').toUpperCase() !== method) continue;
            if (typeof route.path === 'string') {
                if (url.pathname !== route.path) continue;
                return toResponse(await route.respond(url, init, null));
            }
            const match = url.pathname.match(route.path);
            if (!match) continue;
            return toResponse(await route.respond(url, init, match));
        }
        console.warn(`[website mock] unhandled ${method} ${url.pathname}`);
        return notFound();
    };

    tokenStore.set(fakeJwt(uid));

    return () => {
        tokenStore.set(null);
        window.fetch = realFetch;
    };
}

/** Serve a static file from `public/` through the real network stack. */
export function serveStatic(path: string): Promise<Response> {
    return fetch(path);
}
