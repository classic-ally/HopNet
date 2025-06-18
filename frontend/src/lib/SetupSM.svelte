<!-- Setup State Machine -->
<!-- Transition Logic:
 1. InitialSetup, user chooses create or join
 2. Creation flow, user chooses forward or backwards
-->

<script lang="ts">
    import ConfigureDevice from "./ConfigureDevice.svelte";
    import ConfirmPane from "./ConfirmPane.svelte";
    import CreateNetwork from "./CreateNetwork.svelte";
    import InitialSetup from "./InitialSetup.svelte";
    let initialSetup = true;

    // pane 1
    let createNetwork = false;
    let joinNetwork = false;

    // pane 2
    let configureDevice = false;

    // pane 3
    let confirmSelections = false;

    // State variables for user data
    let username = '';
    let password = '';
    let computername = '';
    let ip = '';

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
          bind:username
          bind:password
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
            bind:computername
            bind:ip
            onBackButton={() => {
              createNetwork = true;
              configureDevice = false;
            }}
            onForwardButton={() => {
              confirmSelections = true;
              configureDevice = false;
            }}
          />
      {/if}
      {#if confirmSelections}
            <ConfirmPane
              username={username}
              password={password}
              computername={computername}
              ip={ip}
              onBackButton={() => {
                configureDevice = true;
                confirmSelections = false;
              }}
              onSetupComplete={() => {
                // Setup completed, page will reload automatically
              }}
            />
      {/if}
    {/if}

</div>