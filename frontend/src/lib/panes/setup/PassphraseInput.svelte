<script lang="ts">
    import wordlist from '../../wordlist.json';

    interface Props {
        value: string;
    }

    let {
        value = $bindable('')
    }: Props = $props();

    const WORD_COUNT = 8;
    const MAX_SUGGESTIONS = 6;
    const wordSet = new Set(wordlist as string[]);

    let words = $state<string[]>(Array(WORD_COUNT).fill(''));
    let activeField = $state(-1);
    let suggestions = $state<string[]>([]);
    let selectedSuggestion = $state(-1);
    let validated = $state<(boolean | null)[]>(Array(WORD_COUNT).fill(null));
    let inputRefs: (HTMLInputElement | null)[] = Array(WORD_COUNT).fill(null);
    let blurTimeout: ReturnType<typeof setTimeout> | null = null;
    let suppressSync = false;

    // Sync external value → words
    $effect(() => {
        const v = value;
        if (suppressSync) return;
        if (!v) {
            words = Array(WORD_COUNT).fill('');
            validated = Array(WORD_COUNT).fill(null);
            return;
        }
        const parts = v.split(/\s+/).filter(Boolean);
        const newWords = Array(WORD_COUNT).fill('');
        for (let i = 0; i < Math.min(parts.length, WORD_COUNT); i++) {
            newWords[i] = parts[i];
        }
        words = newWords;
        validated = newWords.map(w => w === '' ? null : wordSet.has(w.toLowerCase()));
    });

    function syncToValue() {
        suppressSync = true;
        value = words.filter(w => w !== '').join(' ');
        // Allow effect to run again after this tick
        queueMicrotask(() => { suppressSync = false; });
    }

    function filterSuggestions(input: string) {
        if (!input) {
            suggestions = [];
            selectedSuggestion = -1;
            return;
        }
        const lower = input.toLowerCase();
        suggestions = (wordlist as string[])
            .filter(w => w.startsWith(lower))
            .slice(0, MAX_SUGGESTIONS);
        selectedSuggestion = suggestions.length > 0 ? 0 : -1;
    }

    function acceptSuggestion(index: number, word: string) {
        words[index] = word;
        validated[index] = true;
        suggestions = [];
        selectedSuggestion = -1;
        syncToValue();
        // Advance to next field
        if (index < WORD_COUNT - 1) {
            focusField(index + 1);
        }
    }

    function focusField(index: number) {
        queueMicrotask(() => {
            inputRefs[index]?.focus();
        });
    }

    function validateField(index: number) {
        const w = words[index].trim().toLowerCase();
        if (w === '') {
            validated[index] = null;
        } else {
            validated[index] = wordSet.has(w);
        }
    }

    function handleInput(index: number) {
        filterSuggestions(words[index]);
        syncToValue();
    }

    function handleKeydown(index: number, event: KeyboardEvent) {
        switch (event.key) {
            case 'ArrowDown':
                if (suggestions.length > 0) {
                    event.preventDefault();
                    selectedSuggestion = Math.min(selectedSuggestion + 1, suggestions.length - 1);
                }
                break;
            case 'ArrowUp':
                if (suggestions.length > 0) {
                    event.preventDefault();
                    selectedSuggestion = Math.max(selectedSuggestion - 1, 0);
                }
                break;
            case 'Enter':
            case 'Tab':
                if (suggestions.length > 0 && selectedSuggestion >= 0) {
                    event.preventDefault();
                    acceptSuggestion(index, suggestions[selectedSuggestion]);
                } else if (event.key === 'Tab' && !event.shiftKey && index < WORD_COUNT - 1) {
                    event.preventDefault();
                    validateField(index);
                    focusField(index + 1);
                } else if (event.key === 'Tab' && event.shiftKey && index > 0) {
                    event.preventDefault();
                    validateField(index);
                    focusField(index - 1);
                }
                break;
            case ' ':
                event.preventDefault();
                if (suggestions.length > 0 && selectedSuggestion >= 0) {
                    acceptSuggestion(index, suggestions[selectedSuggestion]);
                } else if (words[index].trim() && wordSet.has(words[index].trim().toLowerCase())) {
                    words[index] = words[index].trim().toLowerCase();
                    validated[index] = true;
                    suggestions = [];
                    syncToValue();
                    if (index < WORD_COUNT - 1) {
                        focusField(index + 1);
                    }
                }
                break;
            case 'Backspace':
                if (words[index] === '' && index > 0) {
                    event.preventDefault();
                    focusField(index - 1);
                }
                break;
            case 'Escape':
                suggestions = [];
                selectedSuggestion = -1;
                break;
        }
    }

    function handleFocus(index: number) {
        activeField = index;
        filterSuggestions(words[index]);
    }

    function handleBlur(index: number) {
        blurTimeout = setTimeout(() => {
            if (activeField === index) {
                activeField = -1;
                suggestions = [];
                selectedSuggestion = -1;
            }
            validateField(index);
        }, 150);
    }

    function handlePaste(index: number, event: ClipboardEvent) {
        const text = event.clipboardData?.getData('text') ?? '';
        const pastedWords = text.toLowerCase().trim().split(/\s+/).filter(Boolean);
        if (pastedWords.length > 1) {
            event.preventDefault();
            for (let i = 0; i < pastedWords.length && index + i < WORD_COUNT; i++) {
                words[index + i] = pastedWords[i];
                validateField(index + i);
            }
            syncToValue();
            const nextField = Math.min(index + pastedWords.length, WORD_COUNT - 1);
            focusField(nextField);
        }
    }

    function handleSuggestionClick(fieldIndex: number, word: string) {
        if (blurTimeout) {
            clearTimeout(blurTimeout);
            blurTimeout = null;
        }
        acceptSuggestion(fieldIndex, word);
    }

    function borderClass(index: number): string {
        if (validated[index] === true) return 'border-green';
        if (validated[index] === false) return 'border-red';
        return 'border-overlay1';
    }
</script>

<div class="grid grid-cols-2 gap-2">
    {#each words as _, i}
        <div class="relative">
            <div class={`flex items-center gap-1 border border-solid rounded-lg px-2 py-1.5 transition-colors ${borderClass(i)} ${activeField === i ? 'bg-surface0' : 'hover:bg-surface0'}`}>
                <span class="text-overlay0 text-xs w-4 text-right select-none">{i + 1}</span>
                <input
                    bind:this={inputRefs[i]}
                    type="text"
                    class="bg-transparent border-none outline-none text-sm text-text flex-grow min-w-0"
                    placeholder="word"
                    autocomplete="off"
                    autocapitalize="off"
                    spellcheck="false"
                    bind:value={words[i]}
                    oninput={() => handleInput(i)}
                    onkeydown={(e) => handleKeydown(i, e)}
                    onfocus={() => handleFocus(i)}
                    onblur={() => handleBlur(i)}
                    onpaste={(e) => handlePaste(i, e)}
                />
                {#if validated[i] === true}
                    <div class="i-carbon-checkmark text-green text-sm"></div>
                {:else if validated[i] === false}
                    <div class="i-carbon-close text-red text-sm"></div>
                {/if}
            </div>

            {#if activeField === i && suggestions.length > 0}
                <div class="absolute z-10 left-0 right-0 mt-1 bg-mantle rounded-lg border border-solid border-overlay0 overflow-hidden shadow-lg">
                    {#each suggestions as suggestion, si}
                        <button
                            type="button"
                            class={`w-full text-left px-3 py-1.5 text-sm text-text cursor-pointer border-none ${si === selectedSuggestion ? 'bg-surface1' : 'bg-transparent hover:bg-surface0'}`}
                            onmousedown={() => handleSuggestionClick(i, suggestion)}
                        >
                            {suggestion}
                        </button>
                    {/each}
                </div>
            {/if}
        </div>
    {/each}
</div>
