<script lang="ts">
    interface Props {
        icon: string;
        title: string;
        password: boolean;
        value?: string;
        readonly?: boolean;
        multiline?: boolean;
    }

    let inputRef: HTMLInputElement | HTMLTextAreaElement | null = null;
    let isFocused = $state(false);

    function handleClick() {
        if (inputRef && !readonly) {
            inputRef.focus();
        }
    }

    function handleFocus() {
        isFocused = true;
    }

    function handleBlur() {
        isFocused = false;
    }

    function autoResize() {
        if (inputRef && multiline && inputRef instanceof HTMLTextAreaElement) {
            // Reset height to auto to get the correct scrollHeight
            inputRef.style.height = 'auto';
            // Set height to scrollHeight to fit content
            inputRef.style.height = inputRef.scrollHeight + 'px';
        }
    }

    function handleInput() {
        if (multiline) {
            autoResize();
        }
    }

    // Svelte action for auto-resizing
    function autoResizeAction(node: HTMLTextAreaElement) {
        function resize() {
            node.style.height = 'auto';
            node.style.height = node.scrollHeight + 'px';
        }
        
        // Initial resize
        setTimeout(resize, 0);
        
        return {
            destroy() {
                // Cleanup if needed
            }
        };
    }

    // Effect to handle value changes from outside
    $effect(() => {
        if (value && multiline) {
            setTimeout(autoResize, 0);
        }
    });

    let {
        icon,
        title,
        password,
        value = $bindable(''),
        readonly = false,
        multiline = false
    }: Props = $props();
</script>

<style>
    /* Remove default input and textarea focus styles */
    input, textarea {
        outline: none;
        box-shadow: none;
        border: none;
        resize: none; /* Prevent manual resizing of textarea */
    }

    .highlight-on-focus {
        border-color: #3b82f6; /* Indigo highlight */
        outline: 2px solid #3b82f6;
        background-color: rgba(59, 130, 246, 0.1); /* Optional: slight background */
    }
</style>

<div
    class={`flex gap-3 ${multiline ? 'items-start' : 'items-center'} border border-indigo-500 border-solid rounded-lg p-2 ${isFocused && !readonly ? 'highlight-on-focus' : ''} ${readonly ? 'opacity-75 cursor-not-allowed' : 'cursor-pointer'}`}
    role="button"
    onclick={handleClick}
    onkeydown={handleClick}
    tabindex={readonly ? "-1" : "0"}
>
    <div class={icon + " text-xl" + (multiline ? " mt-1" : "")}></div>
    {#if multiline}
        <textarea
            bind:this={inputRef}
            class="bg-transparent text-sm border-none text-white text-base flex-grow min-h-[1.5rem] leading-6"
            placeholder={title}
            onfocus={handleFocus}
            onblur={handleBlur}
            oninput={handleInput}
            {readonly}
            bind:value={value}
            rows="1"
            style="word-break: break-all; overflow-wrap: break-word; overflow: hidden;"
            use:autoResizeAction
        ></textarea>
    {:else}
        <input
            bind:this={inputRef}
            type={password ? 'password' : 'text'}
            class="bg-transparent border-none text-white text-base flex-grow"
            placeholder={title}
            onfocus={handleFocus}
            onblur={handleBlur}
            {readonly}
            bind:value={value}
        />
    {/if}
</div>
