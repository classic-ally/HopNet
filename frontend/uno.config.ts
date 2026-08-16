import presetIcons from '@unocss/preset-icons'

import { defineConfig, presetWind3 } from 'unocss'

// Font families must stay in sync with the @fontsource-variable imports in
// src/main.ts and the :root rule in src/app.css. These are self-hosted from
// node_modules rather than fetched from Google at build time: the Nix build
// sandbox has no network, so a fetching setup silently emits zero @font-face
// rules and the deployed app falls back to the browser default.
const fontSans = '"Red Hat Display Variable", ui-sans-serif, system-ui, sans-serif'
const fontMono = '"Red Hat Mono Variable", ui-monospace, SFMono-Regular, Menlo, monospace'

const themeColors = {
    // Background Colors (Semantic Names)
    base: '#1e1e2e',         // Main background
    mantle: '#181825',       // Darker background (sidebars, secondary areas)
    crust: '#11111b',        // Darkest background (deepest elements)
    surface0: '#313244',     // Elevated surfaces (cards, modals)
    surface1: '#45475a',     // More elevated surfaces (dropdowns, tooltips, buttons)
    surface2: '#585b70',     // Hover states and active states
    surface3: '#6c7086',     // Pressed states (temporary while clicking)

    // Border Colors (Semantic Names)
    overlay0: '#6c7086',     // Dark borders (subtle separation)
    overlay1: '#7f849c',     // Medium borders (clear separation)
    overlay2: '#9399b2',     // Light borders (active/focus states)

    // Text Colors (Semantic Names)
    text: '#cdd6f4',         // Default text (same as primary)
    primary: '#cdd6f4',      // Main text
    subtitle: '#bac2de',     // Secondary text
    muted: '#a6adc8',        // Muted text
    disabled: '#9399b2',     // Disabled text

    // Accent Colors (Essential Only)
    mauve: '#cba6f7',        // Primary brand color
    blue: '#89b4fa',         // Links and info
    green: '#a6e3a1',        // Success states
    yellow: '#f9e2af',       // Warning states
    red: '#f38ba8',          // Error states and destructive actions
    peach: '#fab387',        // Highlights and attention
};

// Generate safelist for all color combinations
const colorNames = Object.keys(themeColors);
const opacities = ['5', '10', '20', '30', '40', '50', '60', '70', '80', '90'];

const colorSafelist = colorNames.flatMap(color => [
    `bg-${color}`,
    `text-${color}`,
    `border-${color}`,
    `hover:border-${color}`,
    `hover:border-${color}/80`,
    `active:bg-${color}`,
    `hover:bg-${color}`,
    ...opacities.flatMap(opacity => [
        `bg-${color}/${opacity}`,
        `border-${color}/${opacity}`,
    ])
]);

export default defineConfig({
    content: {
        filesystem: ['src/**/*.{svelte,ts,js}'],
        pipeline: {
            include: [
                // Default UnoCSS extractors
                /<[^>]*\s[^>]*>/g,                                  // HTML tags with attributes
                /[\w\-.:]+(?:\s*=\s*["'`][^"'`]*["'`])?/g,         // Attributes
                /class[=:]\s*["'`][^"'`]*["'`]/g,                  // class="..." or class: "..."

                // Custom extractors for dynamic icon props
                /icon[=:]\s*["'`](i-carbon-[\w-]+)["'`]/g,         // icon="i-carbon-xxx"
                /\{[\s\S]*?icon\s*:\s*["'`](i-carbon-[\w-]+)["'`]/g,  // { icon: "i-carbon-xxx" }

                // Include Storybook stories
                /\.stories\.(js|ts|svelte)$/,
            ]
        }
    },
    presets: [
        presetIcons(),
        // presetWind3 rather than presetMini: mini is the minimal subset and
        // omits a good deal of the Tailwind vocabulary these components were
        // written against. `space-y-*` was the case that surfaced it — 55 call
        // sites asking for vertical rhythm and getting none, because the rule
        // was never generated. Nothing warns; the class simply does not exist.
        // wind3 extends mini, so this is additive: every utility that resolved
        // before still resolves, and the ones that were silently dead now work.
        // (wind3 is Tailwind v3 semantics, which is what the existing classes
        // assume. wind4 tracks Tailwind v4 and is a separate decision.)
        presetWind3(),
    ],
    theme: {
        colors: themeColors,
        fontFamily: {
            sans: fontSans,
            mono: fontMono,
        }
    },
    safelist: colorSafelist
})