<script lang="ts">
  import { API_BASE_URL } from './stores';

  let ip: string = $state('');
  let isSubmitDisabled = $state(true);

  // State for result
  let result: { address?: string; average_rtt?: number; variance?: number; jitter?: number } = $state({});


  function handleSubmit() {
    if (!ip) return;

    // Make a fetch request to the specified endpoint
    fetch(`${API_BASE_URL}/rpc/get-remote-latency?ip=${encodeURIComponent(ip)}`)
      .then(response => {
        if (!response.ok) throw new Error('Network response was not ok');
        return response.json(); // assuming the endpoint returns JSON
      })
      .then(data => {
        result = {
          address: data.address.split(':')[0], // Strip the port
          average_rtt: data.average_rtt,
          variance: data.variance,
          jitter: data.jitter
        };
      })
      .catch(error => {
        result = { address: `Error: ${error.message}` };
        console.error('There was a problem with the fetch operation:', error);
      });
  }

  function handleInput(e: Event) {
    const target = e.target as HTMLInputElement;
    isSubmitDisabled = !ip;
  }
</script>

<div class="bg-surface0 rounded-lg p-4 inline-block">
  <h1 class="text-lg">This Computer</h1>
  <div class="flex justify-between gap-2 items-center">
    <p class="text-base">Ping</p>
    <input placeholder="Enter IP" type="text" bind:value={ip} oninput={handleInput} class="px-2 h-4"/>
    <button disabled={isSubmitDisabled} onclick={handleSubmit}>Submit</button>
  </div>

  {#if result.address}
    <table class="mt-2 border-collapse border">
      <thead>
        <tr class="bg-surface1">
          <th class="border p-2">Address</th>
          <th class="border p-2">Average RTT (ms)</th>
          <th class="border p-2">Variance</th>
          <th class="border p-2">Jitter (ms)</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td class="border p-2">{result.address}</td>
          <td class="border p-2">{(result.average_rtt || 0).toFixed(6)}</td>
          <td class="border p-2">{result.variance?.toFixed(6) || 'N/A'}</td>
          <td class="border p-2">{result.jitter?.toFixed(6) || 'N/A'}</td>
        </tr>
      </tbody>
    </table>
  {/if}
</div>