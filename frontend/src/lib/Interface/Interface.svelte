<script>
    import AccountSidebarItem from "./AccountSidebarItem.svelte";
    import BrowsePane from "../panes/files/BrowsePane.svelte";
    import Toolbar from "../primitives/Toolbar.svelte";
    import NodesPane from "../panes/nodes/NodesPane.svelte";
    import TakeoutPane from "../panes/takeout/TakeoutPane.svelte";
    import SidebarItem from "./SidebarItem.svelte";
    import ResiliencePane from "../panes/resilience/ResiliencePane.svelte";
    import MaintenancePane from "../panes/maintenance/MaintenancePane.svelte";

    // State to track which sidebar item is selected
    let selectedItem = "browse"; // Can be "recents", "browse", "account", "nodes", "takeout", "resilience", "maintenance"
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

    function handleResilienceClick() {
        selectedItem = "resilience";
        // Close sidebar on mobile after selection
        if (window.innerWidth < 768) {
            isSidebarOpen = false;
        }
    }

    function handleMaintenanceClick() {
        selectedItem = "maintenance";
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
                    <SidebarItem
                        icon="i-carbon-analytics"
                        title="Resilience"
                        selected={selectedItem === "resilience"}
                        onClick={handleResilienceClick}
                    />
                    <SidebarItem
                        icon="i-carbon-clean"
                        title="Maintenance"
                        selected={selectedItem === "maintenance"}
                        onClick={handleMaintenanceClick}
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
            <!-- Browse pane with integrated toolbar -->
            <BrowsePane onToggleSidebar={toggleSidebar}/>
        {:else if selectedItem === "nodes"}
            <!-- Nodes pane with integrated toolbar -->
            <NodesPane onToggleSidebar={toggleSidebar}/>
        {:else if selectedItem === "takeout"}
            <!-- Takeout pane with integrated toolbar -->
            <TakeoutPane onToggleSidebar={toggleSidebar}/>
        {:else if selectedItem === "resilience"}
            <!-- Resilience pane with integrated toolbar -->
            <ResiliencePane onToggleSidebar={toggleSidebar}/>
        {:else if selectedItem === "maintenance"}
            <!-- Maintenance pane with integrated toolbar -->
            <MaintenancePane onToggleSidebar={toggleSidebar}/>
        {:else if selectedItem === "recents"}
            <!-- Minimal toolbar for mobile menu access -->
            <Toolbar
                leftElements={[]}
                centerElements={[]}
                rightElements={[]}
                onToggleSidebar={toggleSidebar}
            />

            <!-- Recents placeholder (TODO: implement RecentsPane) -->
            <div>
                <h3>Recent Files</h3>
                <p class="text-sm text-muted">Your recently accessed files will appear here</p>
            </div>
            <div class="text-muted p-4 text-center">
                No recent files to display
            </div>
        {:else if selectedItem === "account"}
            <!-- Account management will be shown here -->
            <div class="text-muted">Account settings will be shown here</div>
        {/if}
    </div>
</div>
