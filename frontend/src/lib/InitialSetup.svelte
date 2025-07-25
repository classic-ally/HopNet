<script lang="ts">
  import Button from './Button.svelte'
  import FeatureItem from './FeatureItem.svelte'
  import SetupPane from './SetupPane.svelte';

  export let onCreateNetwork: () => void;
  export let onJoinNetwork: () => void;
</script>

<SetupPane 
    title="Welcome."
    body="HopNet is a distributed filesystem designed for everyone, from enthusiast to enterprise."
>
    {#snippet features()}
        <FeatureItem
            icon="i-carbon-3d-print-mesh"
            text="Create a network spanning all your devices, friends, or even an entire company."
        />
        <FeatureItem
            icon="i-carbon-vmdk-disk"
            text="Share disk space across networked devices, allowing you to store more than any one computer can fit."
        />
        <FeatureItem
            icon="i-carbon-volume-block-storage"
            text="Files are split into chunks across the network and stored with redundancy for reliable, high-speed access."
        />
        <FeatureItem
            icon="i-carbon-group-security"
            text="All data is encrypted with your user's key, so other members of the network can't read your files."
        />
    {/snippet}

    {#snippet buttons()}
        <Button
            icon="i-carbon-cloud-services"
            text="Create Network"
            onClick={() => {onCreateNetwork()}}
        />
        <Button
            icon="i-carbon-upstream"
            text="Join Network"
            onClick={() => {onJoinNetwork()}}
        />
    {/snippet}
</SetupPane>