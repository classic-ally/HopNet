<!-- Setup State Machine -->
<!-- Transition Logic:
 1. InitialSetup, user chooses create or join
 2a. Create: CreateNetwork (username + device) → Confirm
 2b. Join: ConfigureDevice → JoinQR
 3. After setup POST, passphrase display → verify → reload
-->

<script lang="ts">
    import { fade } from 'svelte/transition';
    import ConfigureDevice from "./ConfigureDevice.svelte";
    import ConfirmPane from "./ConfirmPane.svelte";
    import CreateNetwork from "./CreateNetwork.svelte";
    import InitialSetup from "./InitialSetup.svelte";
    import JoinQr from "./JoinQR.svelte";
    import PassphraseDisplay from "./PassphraseDisplay.svelte";
    import PassphraseVerify from "./PassphraseVerify.svelte";
    import { ANIM_PANE } from "../../primitives/animation";
    import { tokenStore, API_BASE_URL } from "../../stores";

    let initialSetup = true;

    // create flow
    let createNetwork = false;
    let confirmSelections = false;

    // join flow
    let joinNetwork = false;
    let joinQR = false;

    // passphrase flow
    let passphraseDisplay = false;
    let passphraseVerify = false;

    // State variables for user data
    let username = '';
    let computername = '';
    let passphrase = '';

    /// User just proved possession of the passphrase via the verify step,
    /// so we mint their first session ourselves rather than dumping them on
    /// LoginPane to retype credentials they confirmed seconds ago. On any
    /// failure (network blip, race), fall back to the login pane via reload
    /// so the user has a recoverable path.
    async function autoLoginAndReload() {
        try {
            const response = await fetch(`${API_BASE_URL}/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    username,
                    passphrase,
                    remember_me: true,
                }),
            });
            if (response.ok) {
                const data = await response.json();
                tokenStore.set(data.token);
            }
        } catch (_) {
            // Swallow — reload will route to LoginPane as fallback.
        }
        window.location.reload();
    }
</script>

<div>
    {#if initialSetup}
      <div in:fade={ANIM_PANE}>
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
      </div>
    {/if}
    {#if createNetwork}
      <div in:fade={ANIM_PANE}>
        <CreateNetwork
          bind:username
          bind:computername
          onBackButton={() => {
            initialSetup = true;
            createNetwork = false;
          }}
          onForwardButton={() => {
            confirmSelections = true;
            createNetwork = false;
          }}
        />
      </div>
    {/if}
    {#if joinNetwork}
      <div in:fade={ANIM_PANE}>
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
      </div>
    {/if}
    {#if joinQR}
      <div in:fade={ANIM_PANE}>
        <JoinQr
          name={computername}
        />
      </div>
    {/if}
    {#if confirmSelections}
      <div in:fade={ANIM_PANE}>
        <ConfirmPane
          username={username}
          computername={computername}
          onBackButton={() => {
            createNetwork = true;
            confirmSelections = false;
          }}
          onSetupComplete={(pp) => {
            passphrase = pp;
            confirmSelections = false;
            passphraseDisplay = true;
          }}
        />
      </div>
    {/if}
    {#if passphraseDisplay}
      <div in:fade={ANIM_PANE}>
        <PassphraseDisplay
          passphrase={passphrase}
          onContinue={() => {
            passphraseDisplay = false;
            passphraseVerify = true;
          }}
        />
      </div>
    {/if}
    {#if passphraseVerify}
      <div in:fade={ANIM_PANE}>
        <PassphraseVerify
          passphrase={passphrase}
          onVerified={autoLoginAndReload}
        />
      </div>
    {/if}
</div>
