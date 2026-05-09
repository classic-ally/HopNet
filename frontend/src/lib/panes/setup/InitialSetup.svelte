<script lang="ts">
  import Button from '../../Button.svelte'
  import FeatureItem from './FeatureItem.svelte'
  import SetupPane from '../../SetupPane.svelte';

  export let onCreateNetwork: () => void;
  export let onJoinNetwork: () => void;
</script>

<SetupPane
    logoSrc="/hopnet-logo.png"
    title="Welcome."
>
    {#snippet features()}
        <FeatureItem
            icon="i-carbon-cloud"
            text="Your own cloud, on your existing computers."
        />
        <FeatureItem
            icon="i-carbon-vmdk-disk"
            text="More space than any single computer can hold."
        />
        <FeatureItem
            icon="i-carbon-3d-print-mesh"
            text="Works across your devices, family, or team."
        />
        <FeatureItem
            icon="i-carbon-group-security"
            text="End-to-end encrypted — only you can read your files."
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
