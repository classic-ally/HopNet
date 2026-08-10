<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import Table from './Table.svelte';
  import DateCell from './DateCell.svelte';
  import { TableState } from './tableState.svelte';
  import { getFileIcon } from '../utils/formatters';

  interface DemoRow {
    id: string;
    name: string;
    owner: string;
    size: string; // numeric string, like file_size u64 serialization
    status: 'Ready' | 'Pending' | 'Expired';
    created: string;
  }

  function makeRows(n: number): DemoRow[] {
    const owners = ['allison', 'sam', 'kit'];
    const statuses = ['Ready', 'Pending', 'Expired'] as const;
    return Array.from({ length: n }, (_, i) => ({
      id: `row-${i}`,
      name: `fragment-${String(i).padStart(2, '0')}.bin`,
      owner: owners[i % 3],
      size: String((((i * 37) % 90) + 5) * 10240),
      status: statuses[i % 3],
      created: new Date(Date.UTC(2026, 5, 1 + (i % 28), 9 + (i % 8), 30)).toISOString()
    }));
  }

  const ROWS = makeRows(25);
  const EMPTY: DemoRow[] = [];

  const STATUS_TONES: Record<string, string> = {
    Ready: 'text-green',
    Pending: 'text-yellow',
    Expired: 'text-red'
  };

  // Fresh state per story render, hoisted here so templates stay declarative.
  const basicState = () =>
    new TableState(ROWS, {
      key: (r) => r.id,
      searchFields: (r) => [r.name, r.owner, r.status],
      rowsPerPage: 10
    });
  const sortableState = () => new TableState(ROWS, { key: (r) => r.id, rowsPerPage: 10 });
  const checkboxState = () =>
    new TableState(ROWS, {
      key: (r) => r.id,
      searchFields: (r) => [r.name, r.status],
      rowsPerPage: 10,
      selectable: (r) => r.status !== 'Expired'
    });
  const pointerState = () => new TableState(ROWS.slice(0, 8), { key: (r) => r.id });
  const emptySearchableState = () => new TableState(EMPTY, { searchFields: (r) => [r.name] });
  const bareEmptyState = () => new TableState(EMPTY);
  const paginationState = () => new TableState(ROWS, { key: (r) => r.id, rowsPerPage: 10 });
  const gridState = () => new TableState(ROWS, { key: (r) => r.id, rowsPerPage: 12 });
  const narrowState = () => new TableState(ROWS.slice(0, 6), { key: (r) => r.id });

  const { Story } = defineMeta({
    title: 'Primitives/Table',
    component: Table,
    parameters: {
      docs: {
        description: {
          component:
            'The data-table shell on Card chrome: toolbar row, table or grid body over ' +
            'one TableState, and a footer whose pager actually works (the replaced ' +
            'library never rendered page controls, leaving page 2 unreachable). ' +
            'Columns are config plus per-column cell snippets passed as values.'
        }
      }
    }
  });
</script>

{#snippet statusCell(row)}
  <span class={STATUS_TONES[row.status]}>{row.status}</span>
{/snippet}

{#snippet dateCell(row)}
  <span class="text-sm text-muted"><DateCell date={row.created} /></span>
{/snippet}

{#snippet sizeCell(row)}
  <span class="font-mono text-sm text-muted">{(parseInt(row.size) / 1024).toFixed(1)} KB</span>
{/snippet}

<!-- Search + sort + working pagination over 25 rows. -->
{#snippet basic()}
  <div class="min-h-screen bg-crust p-6">
    <Table
      state={basicState()}
      searchPlaceholder="Search fragments"
      columns={[
        { id: 'name', header: 'Name', sortField: 'name', preset: 'name', field: 'name' },
        { id: 'owner', header: 'Owner', sortField: 'owner', preset: 'status', field: 'owner' },
        { id: 'status', header: 'Status', sortField: 'status', preset: 'status', cell: statusCell },
        { id: 'created', header: 'Created', sortField: 'created', preset: 'date', cell: dateCell }
      ]}
    />
  </div>
{/snippet}

<Story name="Basic" template={basic} />

<!-- The size column sorts numerically via sortValue — string compare put 9 KB above 1000 KB before. -->
{#snippet sortable()}
  <div class="min-h-screen bg-crust p-6">
    <Table
      state={sortableState()}
      columns={[
        { id: 'name', header: 'Name', sortField: 'name', preset: 'name', field: 'name' },
        {
          id: 'size',
          header: 'Size',
          sortField: 'size',
          sortValue: (r) => parseInt(r.size),
          preset: 'size',
          align: 'right',
          cell: sizeCell
        }
      ]}
    />
  </div>
{/snippet}

<Story name="Sortable Numeric" template={sortable} />

<!-- Checkbox selection with a selectable predicate: Expired rows are not selectable. -->
{#snippet checkboxSelection()}
  <div class="min-h-screen bg-crust p-6">
    <Table
      state={checkboxState()}
      selection="checkbox"
      columns={[
        { id: 'name', header: 'Name', sortField: 'name', preset: 'name', field: 'name' },
        { id: 'status', header: 'Status', sortField: 'status', preset: 'status', cell: statusCell }
      ]}
    />
  </div>
{/snippet}

<Story name="Checkbox Selection" template={checkboxSelection} />

<!-- Pointer mode: the pane owns selection policy; here a click just logs. -->
{#snippet pointerSelection()}
  <div class="min-h-screen bg-crust p-6">
    <Table
      state={pointerState()}
      selection="pointer"
      onRowClick={(row) => console.log('click', row.id)}
      onRowDblClick={(row) => console.log('dblclick', row.id)}
      rowClass={(row) => (row.status === 'Ready' ? '' : 'opacity-70')}
      columns={[
        { id: 'name', header: 'Name', preset: 'name', field: 'name' },
        { id: 'status', header: 'Status', preset: 'status', cell: statusCell }
      ]}
    />
  </div>
{/snippet}

<Story name="Pointer Selection" template={pointerSelection} />

{#snippet emptyState()}
  <div class="min-h-screen bg-crust p-6">
    <Table
      state={emptySearchableState()}
      empty="No fragments found. Upload something to get started."
      columns={[{ id: 'name', header: 'Name', preset: 'name', field: 'name' }]}
    />
  </div>
{/snippet}

<Story name="Empty" template={emptyState} />

{#snippet loadingState()}
  <div class="min-h-screen bg-crust p-6">
    <Table
      state={emptySearchableState()}
      loading
      loadingText="Loading fragments..."
      columns={[{ id: 'name', header: 'Name', preset: 'name', field: 'name' }]}
    />
  </div>
{/snippet}

<Story name="Loading" template={loadingState} />

{#snippet errorState()}
  <div class="min-h-screen bg-crust p-6">
    <Table
      state={bareEmptyState()}
      error="Failed to fetch fragments: 503 Service Unavailable"
      onRetry={() => console.log('retry')}
      columns={[{ id: 'name', header: 'Name', preset: 'name', field: 'name' }]}
    />
  </div>
{/snippet}

<Story name="Error With Retry" template={errorState} />

<!-- 25 rows, 10 per page: the pager is the point. Page 2 was unreachable before. -->
{#snippet pagination()}
  <div class="min-h-screen bg-crust p-6">
    <Table
      state={paginationState()}
      columns={[
        { id: 'name', header: 'Name', sortField: 'name', preset: 'name', field: 'name' },
        { id: 'owner', header: 'Owner', preset: 'status', field: 'owner' }
      ]}
    />
  </div>
{/snippet}

<Story name="Pagination" template={pagination} />

{#snippet demoTile(row, ctx)}
  <div class="flex flex-col items-center gap-1 {ctx.selected ? 'ring-2 ring-mauve rounded-lg' : ''}">
    <span class="{getFileIcon('File', row.name, 'detail')} text-4xl text-muted" aria-hidden="true"></span>
    <span class="text-sm text-center break-all">{row.name}</span>
    <span class="text-xs text-muted">{(parseInt(row.size) / 1024).toFixed(0)} KB</span>
  </div>
{/snippet}

<!-- Same state, grid body: tiles via the gridItem snippet, same footer. -->
{#snippet gridView()}
  <div class="min-h-screen bg-crust p-6">
    <Table
      state={gridState()}
      view="grid"
      gridItem={demoTile}
      onRowDblClick={(row) => console.log('open', row.id)}
      columns={[]}
    />
  </div>
{/snippet}

<Story name="Grid View" template={gridView} />

<!-- Narrow container: date column drops to date-only, padding steps down,
     tier-3 columns shrink first. -->
{#snippet narrow()}
  <div class="min-h-screen bg-crust p-6">
    <div class="max-w-md">
      <Table
        state={narrowState()}
        columns={[
          { id: 'name', header: 'Name', sortField: 'name', preset: 'name', field: 'name' },
          { id: 'created', header: 'Created', sortField: 'created', preset: 'date', cell: dateCell },
          { id: 'status', header: 'Status', preset: 'status', cell: statusCell }
        ]}
      />
    </div>
  </div>
{/snippet}

<Story name="Narrow Container" template={narrow} />
