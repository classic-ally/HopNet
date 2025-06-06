<script lang="ts">
    interface Props {
        icon: string;
        title: string;
        password: boolean;
        value?: string;
    }

    let inputRef: HTMLInputElement | null = null;
    let isFocused = $state(false);

    function handleClick() {
        if (inputRef) {
            inputRef.focus();
        }
    }

    function handleFocus() {
        isFocused = true;
    }

    function handleBlur() {
        isFocused = false;
    }

    const { 
        icon, 
        title, 
        password, 
        value = $bindable('') 
    }: Props = $props();
</script>

<style>
    /* Remove default input focus styles */
    input {
        outline: none;
        box-shadow: none;
        border: none;
    }

    .highlight-on-focus {
        border-color: #3b82f6; /* Indigo highlight */
        outline: 2px solid #3b82f6;
        background-color: rgba(59, 130, 246, 0.1); /* Optional: slight background */
    }
</style>

<div 
    class={`flex gap-3 items-center border border-indigo-500 border-solid rounded-lg p-2 ${isFocused ? 'highlight-on-focus' : ''}`} 
    role="button"
    onclick={handleClick}
    onkeydown={handleClick}
    tabindex=""
>
    <div class={icon + " text-xl"}></div>
    <input 
        bind:this={inputRef}
        type={password ? 'password' : 'text'} 
        class="bg-transparent border-none text-white text-base flex-grow" 
        placeholder={title}
        onfocus={handleFocus}
        onblur={handleBlur}
    />
</div>
