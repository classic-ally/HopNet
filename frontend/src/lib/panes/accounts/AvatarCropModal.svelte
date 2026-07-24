<script lang="ts">
    import Modal from '../../primitives/Modal.svelte';
    import Button from '../../Button.svelte';
    import { uploadAvatar } from '../../api/accounts';
    import { refreshCurrentUser } from '../../stores';

    interface AvatarCropModalProps {
        isOpen: boolean;
        onClose: () => void;
    }

    let {
        isOpen,
        onClose,
    }: AvatarCropModalProps = $props();

    let step: 'pick' | 'crop' = $state('pick');
    let loading = $state(false);
    let error = $state('');
    let canvas: HTMLCanvasElement | undefined = $state();
    let imageEl: HTMLImageElement | null = $state(null);

    // Crop state
    let imgW = $state(0);
    let imgH = $state(0);
    let scale = $state(1);
    let cropX = $state(0);
    let cropY = $state(0);
    let cropSize = $state(128);
    let dragging: 'move' | 'resize' | null = $state(null);
    let dragStartX = 0;
    let dragStartY = 0;
    let dragStartCropX = 0;
    let dragStartCropY = 0;
    let dragStartCropSize = 0;

    const CANVAS_MAX = 450;

    function resetState() {
        step = 'pick';
        loading = false;
        error = '';
        imageEl = null;
    }

    function handleClose() {
        resetState();
        onClose();
    }

    function handleFile(file: File) {
        if (!file.type.startsWith('image/')) {
            error = 'Please select an image file';
            return;
        }
        if (file.size > 15_000_000) {
            error = 'Image must be under 15 MB';
            return;
        }
        error = '';
        const url = URL.createObjectURL(file);
        const img = new Image();
        img.onload = () => {
            imageEl = img;
            imgW = img.naturalWidth;
            imgH = img.naturalHeight;
            scale = Math.min(CANVAS_MAX / imgW, CANVAS_MAX / imgH, 1);
            const minDim = Math.min(imgW, imgH);
            cropSize = Math.floor(minDim * 0.8);
            cropX = Math.floor((imgW - cropSize) / 2);
            cropY = Math.floor((imgH - cropSize) / 2);
            step = 'crop';
            // Draw after DOM updates
            requestAnimationFrame(drawCanvas);
        };
        img.src = url;
    }

    function handleInputChange(e: Event) {
        const input = e.target as HTMLInputElement;
        if (input.files?.[0]) handleFile(input.files[0]);
    }

    function handleDrop(e: DragEvent) {
        e.preventDefault();
        if (e.dataTransfer?.files?.[0]) handleFile(e.dataTransfer.files[0]);
    }

    function drawCanvas() {
        if (!canvas || !imageEl) return;
        const ctx = canvas.getContext('2d');
        if (!ctx) return;

        const dw = Math.floor(imgW * scale);
        const dh = Math.floor(imgH * scale);
        canvas.width = dw;
        canvas.height = dh;

        // Draw image
        ctx.drawImage(imageEl, 0, 0, dw, dh);

        // Semi-transparent overlay
        ctx.fillStyle = 'rgba(0, 0, 0, 0.5)';
        ctx.fillRect(0, 0, dw, dh);

        // Clear crop region
        const sx = cropX * scale;
        const sy = cropY * scale;
        const ss = cropSize * scale;
        ctx.clearRect(sx, sy, ss, ss);
        ctx.drawImage(imageEl, cropX, cropY, cropSize, cropSize, sx, sy, ss, ss);

        // Crop border
        ctx.strokeStyle = '#cdd6f4';
        ctx.lineWidth = 2;
        ctx.strokeRect(sx, sy, ss, ss);

        // Corner handles
        const handleSize = 8;
        ctx.fillStyle = '#cdd6f4';
        for (const [hx, hy] of [[sx, sy], [sx + ss, sy], [sx, sy + ss], [sx + ss, sy + ss]]) {
            ctx.fillRect(hx - handleSize / 2, hy - handleSize / 2, handleSize, handleSize);
        }
    }

    function clampCrop() {
        cropSize = Math.max(32, Math.min(cropSize, imgW, imgH));
        cropX = Math.max(0, Math.min(cropX, imgW - cropSize));
        cropY = Math.max(0, Math.min(cropY, imgH - cropSize));
    }

    function getCanvasPos(e: MouseEvent | Touch): [number, number] {
        if (!canvas) return [0, 0];
        const rect = canvas.getBoundingClientRect();
        return [(e.clientX - rect.left) / scale, (e.clientY - rect.top) / scale];
    }

    function isNearCorner(px: number, py: number): boolean {
        const threshold = 16 / scale;
        const corners = [
            [cropX + cropSize, cropY + cropSize],
            [cropX, cropY + cropSize],
            [cropX + cropSize, cropY],
            [cropX, cropY],
        ];
        return corners.some(([cx, cy]) => Math.abs(px - cx) < threshold && Math.abs(py - cy) < threshold);
    }

    function handlePointerDown(e: MouseEvent) {
        const [px, py] = getCanvasPos(e);
        if (isNearCorner(px, py)) {
            dragging = 'resize';
        } else if (px >= cropX && px <= cropX + cropSize && py >= cropY && py <= cropY + cropSize) {
            dragging = 'move';
        } else {
            return;
        }
        dragStartX = px;
        dragStartY = py;
        dragStartCropX = cropX;
        dragStartCropY = cropY;
        dragStartCropSize = cropSize;
    }

    function handlePointerMove(e: MouseEvent) {
        if (!dragging) return;
        const [px, py] = getCanvasPos(e);
        if (dragging === 'move') {
            cropX = dragStartCropX + (px - dragStartX);
            cropY = dragStartCropY + (py - dragStartY);
        } else {
            const dx = px - dragStartX;
            const dy = py - dragStartY;
            const delta = Math.max(dx, dy);
            cropSize = dragStartCropSize + delta;
        }
        clampCrop();
        drawCanvas();
    }

    function handlePointerUp() {
        dragging = null;
    }

    // Touch support
    function handleTouchStart(e: TouchEvent) {
        if (e.touches.length !== 1) return;
        e.preventDefault();
        const touch = e.touches[0];
        const [px, py] = getCanvasPos(touch);
        if (isNearCorner(px, py)) {
            dragging = 'resize';
        } else if (px >= cropX && px <= cropX + cropSize && py >= cropY && py <= cropY + cropSize) {
            dragging = 'move';
        } else {
            return;
        }
        dragStartX = px;
        dragStartY = py;
        dragStartCropX = cropX;
        dragStartCropY = cropY;
        dragStartCropSize = cropSize;
    }

    function handleTouchMove(e: TouchEvent) {
        if (!dragging || e.touches.length !== 1) return;
        e.preventDefault();
        const [px, py] = getCanvasPos(e.touches[0]);
        if (dragging === 'move') {
            cropX = dragStartCropX + (px - dragStartX);
            cropY = dragStartCropY + (py - dragStartY);
        } else {
            const dx = px - dragStartX;
            const dy = py - dragStartY;
            const delta = Math.max(dx, dy);
            cropSize = dragStartCropSize + delta;
        }
        clampCrop();
        drawCanvas();
    }

    function handleTouchEnd() {
        dragging = null;
    }

    async function handleUpload() {
        if (!imageEl) return;
        loading = true;
        error = '';

        try {
            // Render cropped region to offscreen canvas
            const offscreen = document.createElement('canvas');
            offscreen.width = cropSize;
            offscreen.height = cropSize;
            const ctx = offscreen.getContext('2d')!;
            ctx.drawImage(imageEl, cropX, cropY, cropSize, cropSize, 0, 0, cropSize, cropSize);

            const blob = await new Promise<Blob>((resolve, reject) => {
                offscreen.toBlob(
                    (b) => b ? resolve(b) : reject(new Error('Canvas export failed')),
                    'image/jpeg',
                    0.85
                );
            });

            const response = await uploadAvatar(blob);
            if (!response.ok) throw new Error(`Upload failed: ${response.status}`);

            await refreshCurrentUser();
            handleClose();
        } catch (err) {
            error = err instanceof Error ? err.message : 'Upload failed';
        } finally {
            loading = false;
        }
    }

    $effect(() => {
        if (step === 'crop' && canvas && imageEl) {
            drawCanvas();
        }
    });
</script>

{#if isOpen}
    <Modal
        title={step === 'pick' ? 'Change Avatar' : 'Crop Avatar'}
        size="md"
        onClose={handleClose}
        {loading}
        {error}
    >
        {#snippet content()}
            {#if step === 'pick'}
                <div
                    class="border-2 border-dashed border-overlay1 rounded-lg p-8 text-center cursor-pointer hover:border-mauve transition-colors"
                    ondrop={handleDrop}
                    ondragover={(e) => e.preventDefault()}
                    role="button"
                    tabindex="0"
                    onkeydown={(e) => e.key === 'Enter' && document.getElementById('avatar-input')?.click()}
                    onclick={() => document.getElementById('avatar-input')?.click()}
                >
                    <div class="i-carbon-cloud-upload w-10 h-10 mx-auto mb-3 text-muted"></div>
                    <p class="text-muted mb-1">Drop an image here or click to browse</p>
                    <p class="text-sm text-overlay1">JPEG, PNG, or WebP up to 15 MB</p>
                    <input
                        id="avatar-input"
                        type="file"
                        accept="image/*"
                        class="hidden"
                        onchange={handleInputChange}
                    />
                </div>
            {:else}
                <div class="flex justify-center">
                    <canvas
                        bind:this={canvas}
                        class="cursor-crosshair rounded"
                        style="max-width: {CANVAS_MAX}px; max-height: {CANVAS_MAX}px;"
                        onmousedown={handlePointerDown}
                        onmousemove={handlePointerMove}
                        onmouseup={handlePointerUp}
                        onmouseleave={handlePointerUp}
                        ontouchstart={handleTouchStart}
                        ontouchmove={handleTouchMove}
                        ontouchend={handleTouchEnd}
                    ></canvas>
                </div>
            {/if}
        {/snippet}

        {#snippet footer()}
            <div class="flex justify-end gap-2">
                <Button
                    icon="i-carbon-close"
                    text="Cancel"
                    onClick={handleClose}
                    disabled={loading}
                />
                {#if step === 'crop'}
                    <Button
                        icon="i-carbon-upload"
                        text="Upload"
                        onClick={handleUpload}
                        disabled={loading}
                    />
                {/if}
            </div>
        {/snippet}
    </Modal>
{/if}
