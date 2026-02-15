<!-- Setup State Machine -->
<!-- Transition Logic:
 1. InitialSetup, user chooses create or join
 2. Creation flow, user chooses forward or backwards
 3. After setup POST, passphrase display → verify → reload
-->

<script lang="ts">
    import ConfigureDevice from "./ConfigureDevice.svelte";
    import ConfirmPane from "./ConfirmPane.svelte";
    import CreateNetwork from "./CreateNetwork.svelte";
    import InitialSetup from "./InitialSetup.svelte";
    import JoinQr from "./JoinQR.svelte";
    import PassphraseDisplay from "./PassphraseDisplay.svelte";
    import PassphraseVerify from "./PassphraseVerify.svelte";
    let initialSetup = true;

    // pane 1
    let createNetwork = false;
    let joinNetwork = false;

    // pane 2
    let configureDevice = false;
    let joinQR = false;

    // pane 3
    let confirmSelections = false;

    // pane 4 - passphrase flow
    let passphraseDisplay = false;
    let passphraseVerify = false;

    // State variables for user data
    let username = '';
    let computername = '';
    let passphrase = '';

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
      {#if joinNetwork}
          <ConfigureDevice
            bind:computername
            onBackButton={() => {
              initialSetup = true;
              joinNetwork = false;
            }}
            onForwardButton={() => {
              joinQR = true;
              joinNetwork = false;
            }}
          />
      {/if}
      {#if configureDevice}
          <ConfigureDevice
            bind:computername
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
      {#if joinQR}
          <JoinQr
            name={computername}
          />
      {/if}
      {#if confirmSelections}
            <ConfirmPane
              username={username}
              computername={computername}
              onBackButton={() => {
                configureDevice = true;
                confirmSelections = false;
              }}
              onSetupComplete={(pp) => {
                passphrase = pp;
                confirmSelections = false;
                passphraseDisplay = true;
              }}
            />
      {/if}
      {#if passphraseDisplay}
            <PassphraseDisplay
              passphrase={passphrase}
              onContinue={() => {
                passphraseDisplay = false;
                passphraseVerify = true;
              }}
            />
      {/if}
      {#if passphraseVerify}
            <PassphraseVerify
              passphrase={passphrase}
              onVerified={() => {
                window.location.reload();
              }}
            />
      {/if}
    {/if}

</div>
