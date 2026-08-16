import type { Crumb } from './Breadcrumb.svelte';

/**
 * Build a Breadcrumb trail for a filesystem folder path, rooted at a home
 * icon. Shared so the browse pane and the modals that target a directory
 * cannot drift apart in how they describe the same location.
 *
 * Pass `hrefFor` to make the crumbs real links (the browse pane, where a
 * folder is a URL). Omit it for a destination that is not navigable — a
 * modal's target directory, which changes the modal's own state rather than
 * moving the page.
 */
export function crumbsForFolder(folder: string, hrefFor?: (path: string) => string): Crumb[] {
    const trail: Crumb[] = [
        {
            label: 'Home',
            value: '/',
            href: hrefFor?.('/'),
            icon: 'i-carbon-home',
            iconOnly: true,
        },
    ];

    let built = '';
    for (const segment of folder.split('/').filter(Boolean)) {
        built += '/' + segment;
        trail.push({ label: segment, value: built, href: hrefFor?.(built) });
    }

    return trail;
}
