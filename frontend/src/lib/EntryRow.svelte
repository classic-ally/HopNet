<script lang="ts">
    interface Props {
        icon: string;
        title: string;
        password: boolean;
        value?: string;
    }

    const { 
        icon, 
        title, 
        password, 
        value = $bindable('') 
    }: Props = $props();
</script>

<div class="flex gap-3 items-center border border-indigo-500 border-solid rounded-lg p-2">
    <div class={icon + " text-xl"}></div>
    <input type={password ? 'password' : 'text'} class="bg-transparent border-none text-white text-base flex-grow" placeholder={title}/>
</div>