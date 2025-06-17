<!-- we are gonna make this have a network dropdown
- use rust library to extract systen network interfaces
- list filtered interfaces (don't care about loopback, internal interfaces) 
- show network name as title and IP as subtitle
- can use icons for different interface types
-->

<script lang="ts">
    import { onMount } from 'svelte';

    interface NetworkInterface {
        name: string;
        addr: Array<{
            V4?: {
                ip: string;
                broadcast: string;
                netmask: string;
            };
            V6?: {
                ip: string;
                broadcast: string | null;
                netmask: string;
            };
        }>;
        mac_addr: string;
        index: number;
    }

    interface ProcessedInterface {
        type: 'ethernet' | 'wifi' | 'loopback' | 'vpn' | 'other';
        interface: string;
        ip: string;
    }

    interface Props {
        icon: string;
        title: string;
        selected?: string;
    }

    let isOpen = $state(false);
    let isFocused = $state(false);
    let focusedItemIndex = $state(-1);
    let interfaces = $state<ProcessedInterface[]>([]);
    let loading = $state(true);
    let error = $state<string | null>(null);

    function toggleDropdown() {
        isOpen = !isOpen;
    }

    function handleFocus() {
        isFocused = true;
    }

    function handleBlur() {
        isFocused = false;
    }

    function handleItemFocus(index: number) {
        focusedItemIndex = index;
    }

    function handleItemBlur() {
        focusedItemIndex = -1;
    }

    function handleKeydown(event: KeyboardEvent) {
        if (event.key === ' ' || event.key === 'Enter') {
            event.preventDefault();
            toggleDropdown();
        }
    }

    function handleInterfaceKeydown(event: KeyboardEvent, index: number) {
        if (event.key === ' ' || event.key === 'Enter') {
            event.preventDefault();
            selectInterface(index);
        }
    }

    function selectInterface(index: number) {
        // You can emit an event or update selected interface
        selected = interfaces[index].ip;
        isOpen = false;
    }

    function getInterfaceIcon(type: string): string {
        switch (type) {
            case 'ethernet':
                return 'i-carbon-plug';
            case 'wifi':
                return 'i-carbon-wifi';
            case 'loopback':
                return 'i-carbon-edt-loop';
            case 'vpn':
                return 'i-carbon-vpn';
            default:
                return 'i-carbon-unknown';
        }
    }

    function getFirstIPAddress(addr: NetworkInterface['addr']): string {
        if (!addr || addr.length === 0) return 'No IP';
        
        // Prefer IPv4 addresses
        const ipv4 = addr.find(a => a.V4);
        if (ipv4?.V4) return ipv4.V4.ip;
        
        // Fall back to IPv6
        const ipv6 = addr.find(a => a.V6);
        if (ipv6?.V6) return ipv6.V6.ip;
        
        return 'No IP';
    }

    async function fetchInterfaces() {
        try {
            loading = true;
            error = null;
            
            const response = await fetch('http://localhost:34632/interfaces');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data: NetworkInterface[] = await response.json();
            
            // Transform the API response to our interface format
            interfaces = data.map(iface => ({
                type: 'other' as const, // Mark all as "other" as requested
                interface: iface.name,
                ip: getFirstIPAddress(iface.addr)
            }));
            
        } catch (err) {
            error = err instanceof Error ? err.message : 'Failed to fetch interfaces';
            console.error('Error fetching interfaces:', err);
        } finally {
            loading = false;
        }
    }

    onMount(() => {
        fetchInterfaces();
    });

    let {
        icon,
        title,
        selected = $bindable()
    }: Props = $props();
</script>

<div class="relative">
    <div
        class={`flex border border-indigo-500 border-solid rounded-lg gap-3 p-2 cursor-pointer items-center ${isFocused ? 'highlight-on-focus' : ''}`}
        onclick={toggleDropdown}
        onkeydown={handleKeydown}
        onfocus={handleFocus}
        onblur={handleBlur}
        role="button"
        tabindex="0"
    >
        <div class={icon + " text-xl"}></div>
        {#if !selected}
            <div class="text-base text-gray">
                {title}
            </div>
        {:else}
            <div class="text-base">
                {selected}
            </div>
        {/if}
    </div>
    
    {#if isOpen}
        <div
            class="absolute mt-1 w-full border-indigo-900 border-solid rounded-lg bg-blue-950 z-50"
        >
            {#if loading}
                <div class="dropdown-item">
                    <div class="ml-2">
                        <div class="font-medium">Loading interfaces...</div>
                    </div>
                </div>
            {:else if error}
                <div class="dropdown-item">
                    <div class="ml-2">
                        <div class="font-medium text-red-500">Error: {error}</div>
                        <button
                            class="text-sm text-blue-500 hover:underline"
                            onclick={fetchInterfaces}
                        >
                            Retry
                        </button>
                    </div>
                </div>
            {:else if interfaces.length === 0}
                <div class="dropdown-item">
                    <div class="ml-2">
                        <div class="font-medium">No interfaces found</div>
                    </div>
                </div>
            {:else}
                {#each interfaces as thisinterface, index}
                    <div
                        onclick={() => selectInterface(index)}
                        onkeydown={(event: KeyboardEvent) => handleInterfaceKeydown(event, index)}
                        onfocus={() => handleItemFocus(index)}
                        onblur={handleItemBlur}
                        role="button"
                        tabindex="0"
                        class={`dropdown-item ${focusedItemIndex === index ? 'highlight-on-focus' : ''}`}
                    >
                        <span class={getInterfaceIcon(thisinterface.type)}></span>
                        <div class="ml-2 flex flex-1 flex-row justify-between">
                            <div class="font-medium">{thisinterface.interface}</div>
                            <div class="text-sm text-gray-500">{thisinterface.ip}</div>
                        </div>
                    </div>
                {/each}
            {/if}
        </div>
    {/if}

</div>
<style>
    /* .dropdown-content {
        position: absolute;
        top: 100%;
        left: 0;
        right: 0;
        background-color: #f0f9ff;
        border: 2px solid #3b82f6;
        border-radius: 0.5rem;
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
        z-index: 1000;
        max-height: 300px;
        overflow-y: auto;
        min-width: 200px;
        margin-top: 4px;
    } */

    .highlight-on-focus {
        border-color: #3b82f6; /* Indigo highlight */
        outline: 2px solid #3b82f6;
        background-color: rgba(59, 130, 246, 0.1); /* Optional: slight background */
    }

    .dropdown-item {
        display: flex;
        align-items: center;
        padding: 0.5rem;
        cursor: pointer;
        border-radius: 0.375rem; /* Add border radius for better focus ring appearance */
        margin: 0.125rem; /* Small margin to prevent focus ring overlap */
    }
</style>