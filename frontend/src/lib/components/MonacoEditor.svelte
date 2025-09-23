<script lang="ts">
    import { onMount, onDestroy } from 'svelte'
    import * as monaco from 'monaco-editor'

    // Import basic language support - these are self-registering
    import 'monaco-editor/esm/vs/basic-languages/javascript/javascript.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/typescript/typescript.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/css/css.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/html/html.contribution.js'
    // JSON support is built-in to Monaco, no separate import needed
    import 'monaco-editor/esm/vs/basic-languages/python/python.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/rust/rust.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/go/go.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/java/java.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/cpp/cpp.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/csharp/csharp.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/php/php.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/ruby/ruby.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/sql/sql.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/xml/xml.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/yaml/yaml.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/markdown/markdown.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/shell/shell.contribution.js'
    import 'monaco-editor/esm/vs/basic-languages/dockerfile/dockerfile.contribution.js'

    export let value: string = ''
    export let language: string = 'plaintext'
    export let theme: string = 'catppuccin-mocha'
    export let readOnly: boolean = true
    export let options: monaco.editor.IStandaloneEditorConstructionOptions = {}

    let container: HTMLElement
    let editor: monaco.editor.IStandaloneCodeEditor | null = null

    // Catppuccin Mocha color palette
    const catppuccinMocha = {
        base: '#1e1e2e',
        mantle: '#181825',
        crust: '#11111b',
        text: '#cdd6f4',
        subtext1: '#bac2de',
        subtext0: '#a6adc8',
        overlay2: '#9399b2',
        overlay1: '#7f849c',
        overlay0: '#6c7086',
        surface2: '#585b70',
        surface1: '#45475a',
        surface0: '#313244',
        red: '#f38ba8',
        maroon: '#eba0ac',
        peach: '#fab387',
        yellow: '#f9e2af',
        green: '#a6e3a1',
        teal: '#94e2d5',
        sky: '#89dceb',
        sapphire: '#74c7ec',
        blue: '#89b4fa',
        lavender: '#b4befe',
        mauve: '#cba6f7',
        pink: '#f5c2e7',
    }

    function defineCatppuccinTheme() {
        monaco.editor.defineTheme('catppuccin-mocha', {
            base: 'vs-dark',
            inherit: true,
            rules: [
                { token: 'comment', foreground: catppuccinMocha.overlay0 },
                { token: 'keyword', foreground: catppuccinMocha.mauve },
                { token: 'string', foreground: catppuccinMocha.green },
                { token: 'number', foreground: catppuccinMocha.peach },
                { token: 'regexp', foreground: catppuccinMocha.pink },
                { token: 'operator', foreground: catppuccinMocha.sky },
                { token: 'namespace', foreground: catppuccinMocha.yellow },
                { token: 'type', foreground: catppuccinMocha.yellow },
                { token: 'struct', foreground: catppuccinMocha.yellow },
                { token: 'class', foreground: catppuccinMocha.yellow },
                { token: 'interface', foreground: catppuccinMocha.yellow },
                { token: 'parameter', foreground: catppuccinMocha.maroon },
                { token: 'variable', foreground: catppuccinMocha.text },
                { token: 'function', foreground: catppuccinMocha.blue },
                { token: 'member', foreground: catppuccinMocha.blue },
                { token: 'property', foreground: catppuccinMocha.blue },
                { token: 'tag', foreground: catppuccinMocha.peach },
                { token: 'attribute.name', foreground: catppuccinMocha.mauve },
                { token: 'attribute.value', foreground: catppuccinMocha.green },
                { token: 'delimiter', foreground: catppuccinMocha.overlay2 },
                { token: 'delimiter.bracket', foreground: catppuccinMocha.overlay2 },
                { token: 'delimiter.parenthesis', foreground: catppuccinMocha.overlay2 },
                { token: 'delimiter.square', foreground: catppuccinMocha.overlay2 },
                { token: 'delimiter.angle', foreground: catppuccinMocha.overlay2 },
            ],
            colors: {
                'editor.background': catppuccinMocha.base,
                'editor.foreground': catppuccinMocha.text,
                'editorLineNumber.foreground': catppuccinMocha.overlay1,
                'editorLineNumber.activeForeground': catppuccinMocha.lavender,
                'editor.selectionBackground': catppuccinMocha.surface2,
                'editor.selectionHighlightBackground': catppuccinMocha.surface1,
                'editor.findMatchBackground': catppuccinMocha.surface1,
                'editor.findMatchHighlightBackground': catppuccinMocha.surface0,
                'editor.hoverHighlightBackground': catppuccinMocha.surface0,
                'editorCursor.foreground': catppuccinMocha.lavender,
                'editorWhitespace.foreground': catppuccinMocha.surface2,
                'editorIndentGuide.background': catppuccinMocha.surface0,
                'editorIndentGuide.activeBackground': catppuccinMocha.surface2,
                'editor.lineHighlightBorder': catppuccinMocha.surface0,
                'editorBracketMatch.background': catppuccinMocha.surface2,
                'editorBracketMatch.border': catppuccinMocha.overlay0,
                'scrollbar.shadow': catppuccinMocha.mantle,
                'scrollbarSlider.background': catppuccinMocha.surface1,
                'scrollbarSlider.hoverBackground': catppuccinMocha.surface2,
                'scrollbarSlider.activeBackground': catppuccinMocha.overlay0,
            }
        })
    }

    const defaultOptions: monaco.editor.IStandaloneEditorConstructionOptions = {
        readOnly,
        theme,
        language,
        value,
        automaticLayout: true,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        wordWrap: 'on',
        fontSize: 14,
        lineHeight: 1.5,
        fontFamily: 'Red Hat Mono, Monaco, Consolas, monospace',
        padding: { top: 16, bottom: 16 },
        renderLineHighlight: 'none',
        hideCursorInOverviewRuler: true,
        overviewRulerBorder: false,
        scrollbar: {
            vertical: 'auto',
            horizontal: 'auto',
            verticalScrollbarSize: 12,
            horizontalScrollbarSize: 12,
        },
    }

    onMount(() => {
        if (container) {
            // Configure Monaco Environment to suppress worker warnings
            if (!window.MonacoEnvironment) {
                window.MonacoEnvironment = {
                    getWorkerUrl: () => {
                        // Return empty data URL to disable workers (we're using basic highlighting only)
                        return 'data:text/javascript;charset=utf-8,self.MonacoEnvironment={baseUrl:""};'
                    }
                }
            }

            // Define the Catppuccin theme before creating the editor
            defineCatppuccinTheme()

            const mergedOptions = { ...defaultOptions, ...options }
            editor = monaco.editor.create(container, mergedOptions)

            // Set the value and language
            editor.setValue(value)
            monaco.editor.setModelLanguage(editor.getModel()!, language)
        }
    })

    onDestroy(() => {
        if (editor) {
            editor.dispose()
        }
    })

    // Reactive updates
    $: if (editor && value !== editor.getValue()) {
        editor.setValue(value)
    }

    $: if (editor && language) {
        monaco.editor.setModelLanguage(editor.getModel()!, language)
    }

    $: if (editor && theme) {
        monaco.editor.setTheme(theme)
    }
</script>

<div bind:this={container} class="w-full h-full"></div>

<style>
    /* Ensure the container takes full space */
    div {
        min-height: 200px;
    }
</style>