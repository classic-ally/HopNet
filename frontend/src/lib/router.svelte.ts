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

/**
 * Routes that carry a trailing sub-path, so the browsed folder lives in the
 * URL and can be bookmarked: /browse/photos/2026.
 */
const SUBPATH_ROUTES = ['/browse'] as const;

const BROWSE_ROUTE = '/browse';

// ── Router singleton ──────────────────────────────────────────────────

class Router {
    #path     = $state(window.location.pathname);
    #intended: string | null = null;
    #listening = false;

    /** Reactive path. Use in $derived() or $effect() to track navigation. */
    get path(): string { return this.#path; }

    // Both writers read the path back from the browser rather than trusting the
    // argument, so #path is always in the same normalised, percent-encoded form
    // the popstate listener produces. Otherwise a caller passing a decoded URL
    // would make paneForPath behave differently before and after a back button.

    /** Push a history entry and navigate. */
    navigate(url: string) {
        history.pushState({}, '', url);
        this.#path = window.location.pathname;
    }

    /** Replace the current history entry (no back-button record). */
    replace(url: string) {
        history.replaceState({}, '', url);
        this.#path = window.location.pathname;
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

/**
 * Pane id for a recognised path, or null if unknown. Sub-path routes match on
 * their prefix, which is what makes a deep folder URL resolve to the browse
 * pane — App and Interface both route through here, so neither needs to know
 * about folder URLs.
 */
export function paneForPath(path: string): string | null {
    const exact = PATH_TO_PANE[path];
    if (exact) return exact;
    for (const route of SUBPATH_ROUTES) {
        // route + '/' rather than route, so '/browsefoo' stays unrecognised.
        if (path.startsWith(route + '/')) return PATH_TO_PANE[route] ?? null;
    }
    return null;
}

/**
 * Folder path → browse URL. Each segment is encoded on its own, so '/' stays
 * the separator and a folder called "2026 trip" or "100%" survives the round
 * trip. A folder name cannot contain '/', so this is lossless.
 */
export function browseUrlFor(folder: string): string {
    const segments = folder.split('/').filter(Boolean).map(encodeURIComponent);
    return segments.length > 0 ? `${BROWSE_ROUTE}/${segments.join('/')}` : BROWSE_ROUTE;
}

/**
 * Browse URL → folder path, '/' for anything that is not a folder URL.
 * A malformed escape (a hand-truncated bookmark like /browse/100%) decodes to
 * the raw segment rather than throwing, so a bad link never blanks the pane.
 */
export function folderFromBrowseUrl(routePath: string): string {
    if (!routePath.startsWith(BROWSE_ROUTE + '/')) return '/';
    const segments = routePath.slice(BROWSE_ROUTE.length + 1).split('/').filter(Boolean);
    if (segments.length === 0) return '/';
    return (
        '/' +
        segments
            .map((segment) => {
                try {
                    return decodeURIComponent(segment);
                } catch {
                    return segment;
                }
            })
            .join('/')
    );
}
