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
    ]
})