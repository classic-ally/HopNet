import presetMini from '@unocss/preset-mini'
import presetIcons from '@unocss/preset-icons'

import presetWebFonts from '@unocss/preset-web-fonts'
import { createLocalFontProcessor } from '@unocss/preset-web-fonts/local'

import { defineConfig } from 'unocss'

export default defineConfig({
    presets: [
        presetWebFonts({
            provider: 'google',
            fonts: {
                sans: 'Red Hat Display',
                mono: 'Red Hat Mono',
            },
            processors: createLocalFontProcessor({
                cacheDir: 'node_modules/.cache/unocss/fonts',
                fontAssetsDir: 'public/assets/fonts',
                fontServeBaseUrl: '/assets/fonts'
            })
        }),
        presetIcons(),
        presetMini(),
    ],
    theme: {
        colors: {
            // Background Colors (Semantic Names)
            base: '#1e1e2e',         // Main background
            mantle: '#181825',       // Darker background (sidebars, secondary areas)
            crust: '#11111b',        // Darkest background (deepest elements)
            surface0: '#313244',     // Elevated surfaces (cards, modals)
            surface1: '#45475a',     // More elevated surfaces (dropdowns, tooltips)
            surface2: '#585b70',     // Highest elevation surfaces (active states)
            
            // Border Colors (Semantic Names)
            overlay0: '#6c7086',     // Dark borders (subtle separation)
            overlay1: '#7f849c',     // Medium borders (clear separation)
            overlay2: '#9399b2',     // Light borders (active/focus states)
            
            // Text Colors (Semantic Names)
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
        }
    }
})