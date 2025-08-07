<script>
    import AccountSidebarItem from "./AccountSidebarItem.svelte";
    import BrowsePane from "./BrowsePane.svelte";
    import FileBrowserHeader from "./FileBrowserHeader.svelte";
    import NodeAddPane from "./NodeAddPane.svelte";
    import NodesHeader from "./NodesHeader.svelte";
    import NodesTable from "./NodesTable.svelte";
    import SidebarItem from "./SidebarItem.svelte";

    // State to track which sidebar item is selected
    let selectedItem = "recents"; // Can be "recents", "browse", or "account"
    
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
        selectedItem = "account";
        // Close sidebar on mobile after selection
        if (window.innerWidth < 768) {
            isSidebarOpen = false;
        }
    }

    function handleNodesClick() {
        selectedItem = "nodes";
        // Close sidebar on mobile after selection
        if (window.innerWidth < 768) {
            isSidebarOpen = false;
        }
    }

    function handleAccountBack() {
        // When back is clicked from account, go back to recents
        selectedItem = "recents";
    }

    let isNodeAddOpen = false;
    
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
        flex flex-col min-w-[200px] border-r-solid border-r-overlay0 bg-mantle
        fixed md:relative h-full z-40
        transition-transform duration-300 ease-in-out
        {isSidebarOpen ? 'translate-x-0' : '-translate-x-full md:translate-x-0'}
    ">
        <div class="flex flex-col gap-3 p-5 h-full overflow-hidden">
            <img src="/hopnet-logo.png" alt="HopNet" class="w-32 h-auto flex-shrink-0" />
            <div class="flex flex-col flex-grow min-h-0 overflow-y-auto">
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
                <SidebarItem
                    icon="i-carbon-ibm-vsi-on-vpc-for-regulated-industries"
                    title="Nodes"
                    selected={selectedItem === "nodes"}
                    onClick={handleNodesClick}
                />
            </div>
            <AccountSidebarItem
                selected={selectedItem === "account"}
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
