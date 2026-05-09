<script lang="ts">
    import type { Snippet } from 'svelte';
    import { fade, scale } from 'svelte/transition';
    import Button from '../Button.svelte';
    import { ANIM_PANE } from './animation';

    interface ModalProps {
        title: string;
        size?: 'sm' | 'md' | 'lg' | 'xl';
        mode?: 'desktop' | 'mobile';
        onClose?: () => void;
        /// Optional back navigation. When set, a back button mirrors the close
        /// button in the top-left of the header. Used by multi-step modals
        /// (e.g. WelcomeModal) so the back affordance lives in the chrome
        /// rather than a per-page footer.
        onBack?: () => void;

        // Content injection via snippets
        content?: Snippet;
        footer?: Snippet;

        // State management
        loading?: boolean;
        error?: string;
        success?: string;

        // Optional customization
        showCloseButton?: boolean;
        closeOnBackdrop?: boolean;
        closeOnEscape?: boolean;
    }

    let {
        title,
        size = 'md',
        mode = 'desktop',
        onClose = () => {},
        onBack = undefined,
        content,
        footer,
        loading = false,
        error = undefined,
        success = undefined,
        showCloseButton = true,
        closeOnBackdrop = true,
        closeOnEscape = true
    } = $props();

    // Size mappings
    const sizeClasses: Record<string, string> = {
        sm: 'w-full max-w-sm',
        md: 'w-full max-w-md',
        lg: 'w-full max-w-lg',
        xl: 'w-full max-w-2xl'
    };

    // Close button variant - compact for desktop (obvious function), mobile for touch
    const closeButtonVariant = $derived(mode === 'mobile' ? 'mobile' : 'compact');

    function handleBackdropClick() {
        if (closeOnBackdrop) {
            onClose();
        }
    }

    function handleKeydown(event: KeyboardEvent) {
        if (event.key === 'Escape' && closeOnEscape) {
            onClose();
        }
    }

    function handleModalClick(event: Event) {
        // Prevent backdrop close when clicking inside modal
        event.stopPropagation();
    }
</script>

<svelte:window onkeydown={handleKeydown} />

<!-- Backdrop -->
<div
    class="fixed inset-0 bg-black bg-opacity-50 z-40 flex items-center justify-center p-4"
    onclick={handleBackdropClick}
    role="button"
    tabindex="-1"
    onkeydown={(e) => e.key === 'Enter' && handleBackdropClick()}
    transition:fade={ANIM_PANE}
>
    <!-- Modal Container -->
    <div
        class="bg-surface0 border border-overlay1 rounded-lg shadow-xl z-50 {sizeClasses[size]} max-h-[90vh] overflow-hidden flex flex-col"
        onclick={handleModalClick}
        onkeydown={(e) => e.key === 'Enter' && e.stopPropagation()}
        role="dialog"
        tabindex="0"
        aria-modal="true"
        aria-labelledby="modal-title"
        transition:scale={{ ...ANIM_PANE, start: 0.96, opacity: 0 }}
    >
            <!-- Header -->
            <div class="flex items-center justify-between p-4 border-b border-overlay0 flex-shrink-0 gap-2">
                <div class="flex items-center gap-2 min-w-0">
                    {#if onBack}
                        <Button
                            icon="i-carbon-arrow-left"
                            text="Back"
                            variant={closeButtonVariant}
                            onClick={onBack}
                            disabled={loading}
                            tooltip="Back"
                        />
                    {/if}
                    <h3 id="modal-title" class="text-lg font-semibold text-primary truncate">{title}</h3>
                </div>
                {#if showCloseButton}
                    <Button
                        icon="i-carbon-close"
                        text="Close"
                        variant={closeButtonVariant}
                        onClick={() => {
                            onClose();
                        }}
                        disabled={loading}
                        tooltip="Close modal"
                    />
                {/if}
            </div>

            <!-- Content Area -->
            <div class="flex-1 overflow-y-auto">
                <div class="p-4 space-y-4">
                    <!-- Error State -->
                    {#if error}
                        <div class="bg-red/20 border border-red rounded-lg p-3">
                            <div class="flex items-start gap-2">
                                <div class="i-carbon-warning text-red flex-shrink-0 mt-0.5"></div>
                                <p class="text-red text-sm">{error}</p>
                            </div>
                        </div>
                    {/if}

                    <!-- Success State -->
                    {#if success}
                        <div class="bg-green/20 border border-green rounded-lg p-3">
                            <div class="flex items-start gap-2">
                                <div class="i-carbon-checkmark text-green flex-shrink-0 mt-0.5"></div>
                                <p class="text-green text-sm">{success}</p>
                            </div>
                        </div>
                    {/if}

                    <!-- Loading State -->
                    {#if loading}
                        <div class="bg-blue/20 border border-blue rounded-lg p-3">
                            <div class="flex items-center gap-2">
                                <div class="i-carbon-circle-dash text-blue animate-spin"></div>
                                <p class="text-blue text-sm">Processing...</p>
                            </div>
                        </div>
                    {/if}

                    <!-- Main Content -->
                    {@render content?.()}
                </div>
            </div>

        <!-- Footer -->
        {#if footer}
            <div class="border-t border-overlay0 p-4 flex-shrink-0">
                {@render footer?.()}
            </div>
        {/if}
    </div>
</div>

<style>
    /* Ensure modal content doesn't interfere with backdrop clicks */
    .modal-content {
        pointer-events: auto;
    }

    /* Smooth scroll for content area */
    .overflow-y-auto {
        scrollbar-width: thin;
        scrollbar-color: #45475a #313244;
    }

    .overflow-y-auto::-webkit-scrollbar {
        width: 6px;
    }

    .overflow-y-auto::-webkit-scrollbar-track {
        background: #313244;
        border-radius: 3px;
    }

    .overflow-y-auto::-webkit-scrollbar-thumb {
        background: #45475a;
        border-radius: 3px;
    }

    .overflow-y-auto::-webkit-scrollbar-thumb:hover {
        background: #585b70;
    }
</style>