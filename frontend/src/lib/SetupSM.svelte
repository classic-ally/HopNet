<!-- Setup State Machine -->
<!-- Transition Logic:
 1. InitialSetup, user chooses create or join
 2. Creation flow, user chooses forward or backwards
-->

<script lang="ts">
    import CreateNetwork from "./CreateNetwork.svelte";
    import InitialSetup from "./InitialSetup.svelte";
    let initialSetup = true;
    let createNetwork = false;
    let joinNetwork = false;
</script>

<div>
    {#if initialSetup}
      <InitialSetup
        onCreateNetwork={() => {
            createNetwork = true;
            initialSetup = false;
        }}
        onJoinNetwork={() => {
            joinNetwork = true;
            initialSetup = false;
        }}
      />
    {:else}
      {#if createNetwork}
        <CreateNetwork
          onBackButton={() => {
            initialSetup = true;
            createNetwork = false;
          }}
        />
      {/if}
    {/if}

</div>