<script module lang="ts">
    /**
     * The steps of the setup flow. Two branches lead off `initial`:
     *
     *   create: create-network → confirm → passphrase-display → passphrase-verify
     *   join:   configure-device → join-qr
     *
     * The join branch has no terminal step of its own — join-qr waits for a
     * peer on the other side to accept the pairing.
     */
    export type SetupStep =
        | 'initial'
        | 'create-network'
        | 'confirm'
        | 'configure-device'
        | 'join-qr'
        | 'passphrase-display'
        | 'passphrase-verify';
</script>

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
    import { tokenStore } from "../../stores";
    import { liveSetupApi, type SetupApi } from "../../api/setup";

    interface Props {
        /**
         * Which step is showing. A single value rather than one flag per step,
         * so two steps cannot be live at once — the previous shape relied on
         * every handler remembering to clear the flag it was leaving.
         * Bindable so stories and tests can enter mid-flow.
         */
        step?: SetupStep;
        /** Injectable network seam; see api/setup.ts. */
        api?: SetupApi;
        /**
         * Called once the user holds a session. The default full reload is what
         * routes the real app out of setup and into the interface. Stories
         * override it, since reloading would take the Storybook iframe with it.
         */
        onComplete?: () => void;
        username?: string;
        computername?: string;
        /**
         * Normally minted by the create branch and left empty until then.
         * Exposed so a story or test can enter at one of the passphrase steps
         * with something to show.
         */
        passphrase?: string;
    }

    let {
        step = $bindable('initial'),
        api = liveSetupApi,
        onComplete = () => window.location.reload(),
        username = $bindable(''),
        computername = $bindable(''),
        passphrase = $bindable(''),
    }: Props = $props();

    /**
     * The user just proved possession of the passphrase, so mint their first
     * session here rather than dumping them on LoginPane to retype credentials
     * they confirmed seconds ago. A failed login is not fatal: fall through to
     * onComplete regardless, which lands them on LoginPane as a recoverable
     * path.
     */
    async function completeSetup() {
        const result = await api.login(username, passphrase, true);
        if (result.ok) tokenStore.set(result.token);
        onComplete();
    }
</script>

<div>
    {#if step === 'initial'}
      <div in:fade={ANIM_PANE}>
        <InitialSetup
          onCreateNetwork={() => { step = 'create-network'; }}
          onJoinNetwork={() => { step = 'configure-device'; }}
        />
      </div>
    {:else if step === 'create-network'}
      <div in:fade={ANIM_PANE}>
        <CreateNetwork
          bind:username
          bind:computername
          onBackButton={() => { step = 'initial'; }}
          onForwardButton={() => { step = 'confirm'; }}
        />
      </div>
    {:else if step === 'configure-device'}
      <div in:fade={ANIM_PANE}>
        <ConfigureDevice
          bind:computername
          onBackButton={() => { step = 'initial'; }}
          onForwardButton={() => { step = 'join-qr'; }}
        />
      </div>
    {:else if step === 'join-qr'}
      <div in:fade={ANIM_PANE}>
        <JoinQr
          name={computername}
          {api}
        />
      </div>
    {:else if step === 'confirm'}
      <div in:fade={ANIM_PANE}>
        <ConfirmPane
          username={username}
          computername={computername}
          {api}
          onBackButton={() => { step = 'create-network'; }}
          onSetupComplete={(pp) => {
            passphrase = pp;
            step = 'passphrase-display';
          }}
        />
      </div>
    {:else if step === 'passphrase-display'}
      <div in:fade={ANIM_PANE}>
        <PassphraseDisplay
          passphrase={passphrase}
          onContinue={() => { step = 'passphrase-verify'; }}
        />
      </div>
    {:else if step === 'passphrase-verify'}
      <div in:fade={ANIM_PANE}>
        <PassphraseVerify
          passphrase={passphrase}
          onVerified={completeSetup}
        />
      </div>
    {/if}
</div>
