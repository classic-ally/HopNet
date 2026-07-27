<script>
    import { onMount } from "svelte";
    import AccountSidebarItem from "./AccountSidebarItem.svelte";
    import BrowsePane from "../panes/files/BrowsePane.svelte";
    import RecentsPane from "../panes/files/RecentsPane.svelte";
    import NodesPane from "../panes/nodes/NodesPane.svelte";
    import TakeoutPane from "../panes/takeout/TakeoutPane.svelte";
    import WelcomeModal from "../panes/onboarding/WelcomeModal.svelte";
    import { computeIncompleteSteps } from "../panes/onboarding/steps";
    import DevicesPane from "../panes/devices/DevicesPane.svelte";
    import AccountsPane from "../panes/accounts/AccountsPane.svelte";
    import SidebarItem from "./SidebarItem.svelte";
    import Banner from "../primitives/Banner.svelte";
    import ResiliencePane from "../panes/resilience/ResiliencePane.svelte";
    import MaintenancePane from "../panes/maintenance/MaintenancePane.svelte";
    import IncomingSharesPane from "../panes/shares/IncomingSharesPane.svelte";
    import { incomingShareCountStore, importStatusStore, currentUserStore } from "../stores";
    import { router, paneForPath, ACCOUNT_PATH_PREFIX } from "../router.svelte";

    // Derive pane id and sidebar mode from URL — single source of truth.
    let selectedItem  = $derived(paneForPath(router.path) ?? 'browse');
    let inAccountMode = $derived(router.path.startsWith(ACCOUNT_PATH_PREFIX));

    // Welcome / onboarding modal — auto-opens once per Interface mount when
    // any onboarding step is incomplete; reopens when user clicks the
    // onboarding banner. Truth source = users.onboarding_flags (replicated
    // via consensus, so cross-device aware) + importStatusStore for live
    // import-substate.
    let showWelcomeModal = false;
    let welcomeAutoChecked = false;
    /** @type {string | undefined} */
    let initialStepFlag = undefined;

    const unsubUser = currentUserStore.subscribe((user) => {
        if (welcomeAutoChecked || !user) return;
        welcomeAutoChecked = true;
        const incomplete = computeIncompleteSteps(user, $importStatusStore);
        if (incomplete.length > 0) {
            showWelcomeModal = true;
            initialStepFlag = undefined; // start at checklist view
        }
    });

    onMount(() => {
        return () => { unsubUser(); };
    });

    function openWelcomeAtChecklist() {
        initialStepFlag = undefined;
        showWelcomeModal = true;
    }

    function openWelcomeAtImport() {
        initialStepFlag = 'ImportOffered';
        showWelcomeModal = true;
    }

    function closeWelcomeModal() {
        showWelcomeModal = false;
    }

    let importActive = $derived($importStatusStore.record?.status === 'Importing'
        || $importStatusStore.record?.status === 'Pending');

    let incompleteCount = $derived($currentUserStore
        ? computeIncompleteSteps($currentUserStore, $importStatusStore).length
        : 0);
    let showOnboardingBanner = $derived(!importActive && incompleteCount > 0);
    
    // State for mobile sidebar toggle
    let isSidebarOpen = false;

    function nav(url) {
        router.navigate(url);
        if (window.innerWidth < 768) isSidebarOpen = false;
    }

    function navStayOpen(url) {
        router.navigate(url);
    }

    function handleAccountBack() {
        router.navigate('/recent');
    }

    let isNodeAddOpen = false;
    let takeoutTableRef;
    let canCreateTakeout = false;
    let canDownloadSelected = false;
    let canDeleteSelected = false;
    let resilienceRef;
    
    function toggleSidebar() {
        isSidebarOpen = !isSidebarOpen;
    }
</script>
<div class="flex text-primary h-screen w-screen relative overflow-hidden">
    
    <!-- Sidebar overlay for mobile -->
    {#if isSidebarOpen}
        <div 
            class="md:hidden fixed inset-0 bg-black bg-opacity-50 z-40"
            on:click={() => isSidebarOpen = false}
        ></div>
    {/if}
    
    <!-- Sidebar -->
    <div class="
        flex flex-col min-w-[220px] border-r-solid border-r-overlay0 bg-mantle
        fixed md:relative h-full z-40
        transition-transform duration-300 ease-in-out
        {isSidebarOpen ? 'translate-x-0' : '-translate-x-full md:translate-x-0'}
    ">
        <div class="flex flex-col gap-3 p-5 h-full overflow-hidden">
            <img src="/hopnet-logo.png" alt="HopNet" class="w-32 h-auto flex-shrink-0" />
            <div class="flex flex-col flex-grow min-h-0 overflow-y-auto">
                {#if inAccountMode}
                    <!-- Account mode: Show Account, Takeouts and Nodes -->
                    <SidebarItem
                        icon="i-carbon-user"
                        title="Accounts"
                        selected={selectedItem === "account"}
                        onClick={() => navStayOpen('/settings/accounts')}
                    />
                    <SidebarItem
                        icon="i-carbon-cloud-download"
                        title="Takeouts"
                        selected={selectedItem === "takeout"}
                        onClick={() => navStayOpen('/settings/takeout')}
                    />
                    <SidebarItem
                        icon="i-carbon-phone"
                        title="Devices"
                        selected={selectedItem === "devices"}
                        onClick={() => navStayOpen('/settings/devices')}
                    />
                    <SidebarItem
                        icon="i-carbon-ibm-vsi-on-vpc-for-regulated-industries"
                        title="Nodes"
                        selected={selectedItem === "nodes"}
                        onClick={() => navStayOpen('/settings/nodes')}
                    />
                    <SidebarItem
                        icon="i-carbon-analytics"
                        title="Resilience"
                        selected={selectedItem === "resilience"}
                        onClick={() => navStayOpen('/settings/resilience')}
                    />
                    <SidebarItem
                        icon="i-carbon-clean"
                        title="Maintenance"
                        selected={selectedItem === "maintenance"}
                        onClick={() => navStayOpen('/settings/maintenance')}
                    />
                {:else}
                    <!-- Default mode: Show Recents and Browse -->
                    <SidebarItem
                        icon="i-carbon-time"
                        title="Recents"
                        selected={selectedItem === "recents"}
                        onClick={() => nav('/recent')}
                    />
                    <SidebarItem
                        icon="i-carbon-folder"
                        title="Browse"
                        selected={selectedItem === "browse"}
                        onClick={() => nav('/browse')}
                    />
                    <SidebarItem
                        icon="i-carbon-collaborate"
                        title="Shared With Me"
                        selected={selectedItem === "shared"}
                        onClick={() => nav('/shared')}
                        badge={$incomingShareCountStore}
                    />
                {/if}
            </div>
            <AccountSidebarItem
                selected={inAccountMode}
                onClick={() => navStayOpen('/settings/accounts')}
                onBack={handleAccountBack}
            />
        </div>
    </div>
    <!-- Main content -->
    <div class="p-5 flex flex-col gap-3 w-full bg-base overflow-auto">
        {#if selectedItem === "browse"}
            <BrowsePane onToggleSidebar={toggleSidebar}/>
        {:else if selectedItem === "nodes"}
            <NodesPane onToggleSidebar={toggleSidebar}/>
        {:else if selectedItem === "takeout"}
            <TakeoutPane onToggleSidebar={toggleSidebar}/>
        {:else if selectedItem === "devices"}
            <DevicesPane onToggleSidebar={toggleSidebar}/>
        {:else if selectedItem === "resilience"}
            <ResiliencePane onToggleSidebar={toggleSidebar}/>
        {:else if selectedItem === "maintenance"}
            <MaintenancePane onToggleSidebar={toggleSidebar}/>
        {:else if selectedItem === "shared"}
            <IncomingSharesPane onToggleSidebar={toggleSidebar}/>
        {:else if selectedItem === "recents"}
            <RecentsPane onToggleSidebar={toggleSidebar}/>
        {:else if selectedItem === "account"}
            <AccountsPane onToggleSidebar={toggleSidebar}/>
        {/if}
    </div>
</div>

<!-- Floating banner stack. Wide viewports: bottom-right toast, capped width.
     Narrow (sidebar collapses below md): full-width across the bottom with
     the same 1.25rem inset as the main content's p-5. -->
<div class="fixed z-30 flex flex-col gap-2 bottom-5 left-5 right-5 md:left-auto md:max-w-sm pointer-events-none">
    {#if importActive}
        <div class="pointer-events-auto">
            <Banner
                variant="info"
                title="Import in progress"
                subtitle={$importStatusStore.counts
                    ? `${$importStatusStore.counts.imported + $importStatusStore.counts.skipped + $importStatusStore.counts.failed} of ${$importStatusStore.counts.total} files processed. Uploads paused until it finishes.`
                    : 'Working on the owner device. Uploads paused until it finishes.'}
                onClick={openWelcomeAtImport}
            />
        </div>
    {:else if showOnboardingBanner}
        <div class="pointer-events-auto">
            <Banner
                variant="info"
                title="Welcome to HopNet"
                subtitle="{incompleteCount} setup task{incompleteCount === 1 ? '' : 's'} left. Click to continue."
                onClick={openWelcomeAtChecklist}
            />
        </div>
    {/if}
</div>

{#if showWelcomeModal && $currentUserStore}
    <WelcomeModal
        user={$currentUserStore}
        importState={$importStatusStore}
        {initialStepFlag}
        onClose={closeWelcomeModal}
    />
{/if}
