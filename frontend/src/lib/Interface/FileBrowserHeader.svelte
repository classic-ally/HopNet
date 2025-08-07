<script>
    import ControlBarIcon from "./ControlBarIcon.svelte";
    import UploadFiles from "./UploadFiles.svelte";
    import CreateFolder from "./CreateFolder.svelte";
    import { refreshTriggerStore } from '../stores';
    import { onMount } from 'svelte';
    
    export let onToggleSidebar;
    
    let showUploadPopover = false;
    let showCreateFolderPopover = false;
    let isMobile = false;
    
    function checkMobile() {
        isMobile = window.innerWidth < 768;
    }
    
    onMount(() => {
        checkMobile();
        window.addEventListener('resize', checkMobile);
        return () => window.removeEventListener('resize', checkMobile);
    });
    
    function handleUploadClick() {
        showUploadPopover = true;
    }
    
    function handleUploadClose() {
        showUploadPopover = false;
    }
    
    function handleFilesUploaded() {
        showUploadPopover = false;
        // Trigger refresh of the browse pane
        refreshTriggerStore.update(n => n + 1);
        console.log('Files uploaded successfully');
    }
    
    function handleCreateFolderClick() {
        showCreateFolderPopover = true;
    }
    
    function handleCreateFolderClose() {
        showCreateFolderPopover = false;
    }
    
    function handleFolderCreated() {
        showCreateFolderPopover = false;
        // Trigger refresh of the browse pane
        refreshTriggerStore.update(n => n + 1);
        console.log('Folder created successfully');
    }
    
    function handleViewModeClick() {
        console.log('View mode clicked');
    }
    
    function handleDownloadClick() {
        console.log('Download clicked');
    }
    
    function handleDeleteClick() {
        console.log('Delete clicked');
    }
    
    function handleShareClick() {
        console.log('Share clicked');
    }
</script>

<div class="flex gap-1">
    <div class="flex flex-1 min-w-0 justify-start gap-1">
        {#if isMobile}
            <ControlBarIcon
                icon="i-carbon-menu"
                title="Toggle sidebar"
                onClick={onToggleSidebar}
            />
        {/if}
        <ControlBarIcon
            icon="i-carbon-list"
            title="Change view mode"
            onClick={handleViewModeClick}
        />
    </div>
    <div class="flex flex-1 min-w-0 justify-center gap-1">
        <ControlBarIcon
            icon="i-carbon-cloud-upload"
            title="Upload..."
            onClick={handleUploadClick}
        />
        <ControlBarIcon
            icon="i-carbon-folder-add"
            title="Add a folder"
            onClick={handleCreateFolderClick}
        />
    </div>
    <div class="flex flex-1 min-w-0 justify-end gap-1">
        <ControlBarIcon
            icon="i-carbon-cloud-download"
            title="Download..."
            onClick={handleDownloadClick}
        />
        <ControlBarIcon
            icon="i-carbon-trash-can"
            title="Delete"
            onClick={handleDeleteClick}
        />
        <ControlBarIcon
            icon="i-carbon-share"
            title="Share"
            onClick={handleShareClick}
        />
    </div>
</div>

<!-- Upload Files Popover -->
<UploadFiles
    bind:isOpen={showUploadPopover}
    on:close={handleUploadClose}
    on:uploaded={handleFilesUploaded}
/>

<!-- Create Folder Popover -->
<CreateFolder
    bind:isOpen={showCreateFolderPopover}
    on:close={handleCreateFolderClose}
    on:created={handleFolderCreated}
/>