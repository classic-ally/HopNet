/**
 * Moves a node to <body> so it escapes any ancestor that clips or scrolls.
 * Required for menus anchored inside a Card, which carries overflow-hidden to
 * keep a full-bleed table's square corners inside its rounded border.
 */
export function portal(node: HTMLElement) {
    document.body.appendChild(node);
    return {
        destroy() {
            node.remove();
        },
    };
}
