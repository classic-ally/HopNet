<script>
    import AccountSidebarItem from "./AccountSidebarItem.svelte";
    import BrowsePane from "./BrowsePane.svelte";
    import FileBrowserHeader from "./FileBrowserHeader.svelte";
    import NodeAddPane from "./NodeAddPane.svelte";
    import NodesHeader from "./NodesHeader.svelte";
    import NodesTable from "./NodesTable.svelte";
    import TakeoutHeader from "./TakeoutHeader.svelte";
    import TakeoutTable from "./TakeoutTable.svelte";
    import SidebarItem from "./SidebarItem.svelte";

    // State to track which sidebar item is selected
    let selectedItem = "recents"; // Can be "recents", "browse", "account", "nodes", "takeout"
    // State to track if we're in account mode (sticky)
    let inAccountMode = false;
    
    // State for mobile sidebar toggle
    let isSidebarOpen = false;

    // Click handlers for sidebar items
    function handleRecentsClick() {
        selectedItem = "recents";
        // Close sidebar on mobile after selection
        if (window.innerWidth < 768) {
            isSidebarOpen = false;
        }
    }

    function handleBrowseClick() {
        selectedItem = "browse";
        // Close sidebar on mobile after selection
        if (window.innerWidth < 768) {
            isSidebarOpen = false;
        }
    }

    function handleAccountClick() {
        inAccountMode = true;
        selectedItem = "account";
        // Don't close sidebar on mobile since user may want to navigate to other settings pages
    }

    function handleNodesClick() {
        selectedItem = "nodes";
        // Close sidebar on mobile after selection
        if (window.innerWidth < 768) {
            isSidebarOpen = false;
        }
    }

    function handleTakeoutClick() {
        selectedItem = "takeout";
        // Close sidebar on mobile after selection
        if (window.innerWidth < 768) {
            isSidebarOpen = false;
        }
    }

    function handleAccountBack() {
        // When back is clicked from account, exit account mode and go back to recents
        inAccountMode = false;
        selectedItem = "recents";
    }

    let isNodeAddOpen = false;
    let takeoutTableRef;
    let canCreateTakeout = false;
    let canDownloadSelected = false;
    let canDeleteSelected = false;
    
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
                        title="Account"
                        selected={selectedItem === "account"}
                        onClick={handleAccountClick}
                    />
                    <SidebarItem
                        icon="i-carbon-cloud-download"
                        title="Takeouts"
                        selected={selectedItem === "takeout"}
                        onClick={handleTakeoutClick}
                    />
                    <SidebarItem
                        icon="i-carbon-ibm-vsi-on-vpc-for-regulated-industries"
                        title="Nodes"
                        selected={selectedItem === "nodes"}
                        onClick={handleNodesClick}
                    />
                {:else}
                    <!-- Default mode: Show Recents and Browse -->
                    <SidebarItem
                        icon="i-carbon-time"
                        title="Recents"
                        selected={selectedItem === "recents"}
                        onClick={handleRecentsClick}
                    />
                    <SidebarItem
                        icon="i-carbon-folder"
                        title="Browse"
                        selected={selectedItem === "browse"}
                        onClick={handleBrowseClick}
                    />
                {/if}
            </div>
            <AccountSidebarItem
                selected={inAccountMode}
                onClick={handleAccountClick}
                onBack={handleAccountBack}
            />
        </div>
    </div>
    <!-- Main content -->
    <div class="p-5 flex flex-col gap-3 w-full bg-base overflow-auto">
        {#if selectedItem === "browse"}
            <!-- Header area -->
            <FileBrowserHeader onToggleSidebar={toggleSidebar}/>
            <!-- Browse pane -->
            <BrowsePane/>
        {:else if selectedItem === "nodes"}
            <NodesHeader
                onAddNode={() => {isNodeAddOpen = !isNodeAddOpen}}
                onToggleSidebar={toggleSidebar}
            />
            <NodesTable/>
            {#if isNodeAddOpen}
                <NodeAddPane
                    onBackButton={() => {isNodeAddOpen = !isNodeAddOpen}}
                />
            {/if}
        {:else if selectedItem === "takeout"}
            <TakeoutHeader
                onToggleSidebar={toggleSidebar}
                onCreateTakeout={() => {
                    if (takeoutTableRef && takeoutTableRef.initiateTakeout) {
                        takeoutTableRef.initiateTakeout();
                    }
                }}
                onDownloadSelected={() => {
                    if (takeoutTableRef && takeoutTableRef.downloadSelectedTakeouts) {
                        takeoutTableRef.downloadSelectedTakeouts();
                    }
                }}
                onDeleteSelected={() => {
                    if (takeoutTableRef && takeoutTableRef.deleteSelectedTakeouts) {
                        takeoutTableRef.deleteSelectedTakeouts();
                    }
                }}
                canCreate={canCreateTakeout}
                canDownloadSelected={canDownloadSelected}
                canDeleteSelected={canDeleteSelected}
            />
            <TakeoutTable bind:this={takeoutTableRef} bind:canCreateTakeout bind:canDownloadSelected bind:canDeleteSelected/>
        {:else if selectedItem === "recents"}
            <!-- Header area -->
            <FileBrowserHeader onToggleSidebar={toggleSidebar}/>
            <!-- Body area -->
            <div class="text-muted">Recent files will be shown here</div>
        {:else if selectedItem === "account"}
            <!-- Account management will be shown here -->
            <div class="text-muted">Account settings will be shown here</div>
        {/if}
    </div>
</div>
