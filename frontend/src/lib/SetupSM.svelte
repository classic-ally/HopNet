<!-- Setup State Machine -->
<!-- Transition Logic:
 1. InitialSetup, user chooses create or join
 2. Creation flow, user chooses forward or backwards
-->

<script lang="ts">
    import ConfigureDevice from "./ConfigureDevice.svelte";
    import CreateNetwork from "./CreateNetwork.svelte";
    import InitialSetup from "./InitialSetup.svelte";
    let initialSetup = true;

    // pane 1
    let createNetwork = false;
    let joinNetwork = false;

    // pane 2
    let configureDevice = false;
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
          onForwardButton={() => {
            configureDevice = true;
            createNetwork = false;
          }}
        />
      {/if}
      {#if configureDevice}
          <ConfigureDevice
            onBackButton={() => {
              createNetwork = true;
              configureDevice = false;
            }}
          />
      {/if}
    {/if}

</div>