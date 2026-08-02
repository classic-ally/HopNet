// Hand-rolled popstate router. __path is $state so any .svelte component
// reading router.path re-renders on navigation. Zero dependencies.

// ── Path ↔ pane id maps ──────────────────────────────────────────────

const PATH_TO_PANE: Record<string, string> = {
    '/recent':             'recents',
    '/browse':             'browse',
    '/shared':             'shared',
    '/photos':             'photos',
    '/settings/accounts':  'account',
    '/settings/nodes':     'nodes',
    '/settings/takeout':   'takeout',
    '/settings/devices':   'devices',
    '/settings/resilience':'resilience',
    '/settings/maintenance':'maintenance',
};

/** Path prefix that signals "account mode" in the sidebar. */
export const ACCOUNT_PATH_PREFIX = '/settings/';

// ── Router singleton ──────────────────────────────────────────────────

class Router {
    #path     = $state(window.location.pathname);
    #intended: string | null = null;
    #listening = false;

    /** Reactive path. Use in $derived() or $effect() to track navigation. */
    get path(): string { return this.#path; }

    /** Push a history entry and navigate. */
    navigate(url: string) {
        history.pushState({}, '', url);
        this.#path = url;
    }

    /** Replace the current history entry (no back-button record). */
    replace(url: string) {
        history.replaceState({}, '', url);
        this.#path = url;
    }

    /** Redirect to /login, stashing current path for post-login restoration. */
    redirectToLogin() {
        const p = this.#path;
        if (p !== '/login' && p !== '/setup') this.#intended = p;
        this.replace('/login');
    }

    /** Navigate to stashed intended path, or /recent if none was set. */
    redirectToIntended() {
        const target = this.#intended ?? '/recent';
        this.#intended = null;
        this.navigate(target);
    }

    /** Wire popstate. Call once at app init. Idempotent. */
    init() {
        if (this.#listening) return;
        this.#listening = true;
        window.addEventListener('popstate', () => {
            this.#path = window.location.pathname;
        });
    }
}

export const router = new Router();

/** Pane id for a recognised path, or null if unknown. */
export function paneForPath(path: string): string | null {
    return PATH_TO_PANE[path] ?? null;
}
