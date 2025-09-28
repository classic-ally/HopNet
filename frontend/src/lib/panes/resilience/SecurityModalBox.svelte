<script lang="ts">
    export let title: string;
    export let bgColor: string;
    export let headerBgColor: string = '';
    export let borderColor: string;
    export let textColor: string;
    export let headerIcon: string;
    export let items: ReadonlyArray<{
        readonly title: string;
        readonly subtitles?: readonly string[];
        readonly icon: string;
    }> = [];
    export let paragraphs: readonly string[] = [];

    // Use headerBgColor if provided, otherwise fall back to bgColor
    $: actualHeaderBg = headerBgColor || bgColor;
</script>

<div class="bg-{bgColor} {borderColor ? `border ${borderColor}` : ''} rounded-lg">
    <div class="bg-{actualHeaderBg} px-4 py-3 rounded-t-lg {borderColor ? `border-b ${borderColor}` : ''}">
        <h4 class="font-medium text-{textColor} flex items-center gap-2">
            <div class="i-carbon-{headerIcon}"></div>
            {title}
        </h4>
    </div>

    {#if paragraphs.length > 0}
        <!-- Paragraph content -->
        <div class="p-4 text-sm text-text">
            {#each paragraphs as paragraph, index}
                <p class="leading-normal {index > 0 ? 'mt-4' : ''}">{paragraph}</p>
            {/each}
        </div>
    {:else}
        <!-- Items content -->
        <div class="p-4 space-y-3">
            {#each items as item}
                <div class="flex items-start gap-2">
                    <div class="i-carbon-{item.icon} text-{textColor} mt-0.5 flex-shrink-0"></div>
                    <div class="min-w-0">
                        <div class="text-{textColor} text-sm font-medium">{item.title}</div>
                        {#if item.subtitles && item.subtitles.length > 0}
                            <ul class="mt-1 space-y-1">
                                {#each item.subtitles as subtitle}
                                    <li class="text-text text-xs leading-relaxed flex items-start">
                                        <span class="mr-1.5">•</span>
                                        <span>{subtitle}</span>
                                    </li>
                                {/each}
                            </ul>
                        {/if}
                    </div>
                </div>
            {/each}
        </div>
    {/if}
</div>