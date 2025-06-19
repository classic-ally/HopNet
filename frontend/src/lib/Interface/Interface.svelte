<script>
    import AccountSidebarItem from "./AccountSidebarItem.svelte";
    import FileBrowserHeader from "./FileBrowserHeader.svelte";
    import NodeAddPane from "./NodeAddPane.svelte";
    import NodesHeader from "./NodesHeader.svelte";
    import NodesTable from "./NodesTable.svelte";
    import SidebarItem from "./SidebarItem.svelte";

    // State to track which sidebar item is selected
    let selectedItem = "recents"; // Can be "recents", "browse", or "account"

    // Click handlers for sidebar items
    function handleRecentsClick() {
        selectedItem = "recents";
    }

    function handleBrowseClick() {
        selectedItem = "browse";
    }

    function handleAccountClick() {
        selectedItem = "account";
    }

    function handleNodesClick() {
        selectedItem = "nodes";
    }

    function handleAccountBack() {
        // When back is clicked from account, go back to recents
        selectedItem = "recents";
    }

    let isNodeAddOpen = false;
</script>
<div class="flex text-white h-screen w-screen">
    <!-- Sidebar -->
    <div class="flex flex-col gap-3 p-5 min-w-[200px] border-r-solid border-r-indigo-950">
        <h2>AppName</h2>
        <div class="flex flex-col flex-grow">
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
    <!-- Main content -->
    <div class="p-5 flex flex-col gap-3 w-full">
        {#if selectedItem != "nodes"}
            <!-- Header area -->
            <FileBrowserHeader/>
            <!-- Body area -->
            test
        {/if}
        {#if selectedItem == "nodes"}
            <NodesHeader
                onAddNode={() => {isNodeAddOpen = !isNodeAddOpen}}
            />
            <NodesTable/>
            {#if isNodeAddOpen}
                <NodeAddPane
                    onBackButton={() => {isNodeAddOpen = !isNodeAddOpen}}
                />
            {/if}
        {/if}
        

    </div>
</div>
