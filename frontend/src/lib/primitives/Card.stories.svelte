<script module lang="ts">
  import { defineMeta } from '@storybook/addon-svelte-csf';
  import Card from './Card.svelte';

  const { Story } = defineMeta({
    title: 'Primitives/Card',
    component: Card,
    argTypes: {
      title: { control: 'text' },
      subtitle: { control: 'text' },
      icon: { control: 'text' },
      padding: { control: 'boolean' }
    },
    parameters: {
      docs: {
        description: {
          component:
            'The grouping surface: bg-surface0 with the subtle overlay0 border, owning ' +
            'background, border and radius in one element. The header row is optional — ' +
            'title, muted subtitle, leading icon, and a right-aligned snippet for status ' +
            'or small actions. Each story below mirrors a real call site it will replace.'
        }
      }
    }
  });
</script>

<!-- Stories sit on the app background so the contrast reads honestly. -->

<!-- ConsensusPanel's header: title with a status reading on the right. -->
{#snippet consensus()}
  <div class="min-h-screen bg-crust p-6">
    <div class="max-w-lg">
      <Card title="State Machine Replication">
        {#snippet headerRight()}
          <span class="text-lg font-semibold text-green">Healthy</span>
        {/snippet}
        <p class="text-sm text-subtitle">
          5 of 7 nodes seated. The pool tolerates 2 simultaneous failures.
        </p>
      </Card>
    </div>
  </div>
{/snippet}

<Story name="Title + Status Right" template={consensus} />

<!-- StoragePanel's header: a mono status fragment on the right. -->
{#snippet storage()}
  <div class="min-h-screen bg-crust p-6">
    <div class="max-w-lg">
      <Card title="Data Replication">
        {#snippet headerRight()}
          <span class="text-xs font-mono">
            <span class="text-subtitle">worst block tolerates</span>
            <span class="text-mauve">2</span>
            <span class="text-subtitle">node failures</span>
          </span>
        {/snippet}
        <div class="h-24 grid place-items-center text-muted text-sm border border-dashed border-overlay0 rounded">
          chart content
        </div>
      </Card>
    </div>
  </div>
{/snippet}

<Story name="Title + Mono Detail" template={storage} />

<!-- ImportProgressCard: leading icon and a subtitle. -->
{#snippet importing()}
  <div class="min-h-screen bg-crust p-6">
    <div class="max-w-lg">
      <Card
        title="Import in progress"
        subtitle="14 of 30 files copied into the mesh"
        icon="i-carbon-circle-dash text-blue animate-spin"
      >
        <div class="w-full bg-surface1 rounded-full h-2">
          <div class="bg-blue h-2 rounded-full" style="width: 47%"></div>
        </div>
      </Card>
    </div>
  </div>
{/snippet}

<Story name="Icon + Subtitle" template={importing} />

<!-- MaintenancePane section: subtitle doing the explanatory work, actions inside. -->
{#snippet maintenance()}
  <div class="min-h-screen bg-crust p-6">
    <div class="max-w-xl">
      <Card
        title="Orphaned Fragment Cleanup"
        subtitle="Finds fragments whose parent file no longer exists and reclaims their space."
      >
        <div class="flex gap-3">
          <button class="text-primary bg-surface1 border border-overlay1 rounded-md px-3 py-1.5 text-sm">
            Scan
          </button>
          <button class="text-primary bg-surface1 border border-overlay1 rounded-md px-3 py-1.5 text-sm">
            Clean up
          </button>
        </div>
      </Card>
    </div>
  </div>
{/snippet}

<Story name="Section With Body Actions" template={maintenance} />

<!-- No title: the card is pure surface, as the resilience wrappers are today. -->
{#snippet headerless()}
  <div class="min-h-screen bg-crust p-6">
    <div class="max-w-lg">
      <Card>
        <p class="text-sm text-subtitle">
          Content that groups itself — the surface and border alone mark the boundary.
        </p>
      </Card>
    </div>
  </div>
{/snippet}

<Story name="Headerless" template={headerless} />

<!-- padding=false: content runs to the edges; the header keeps its own inset. -->
{#snippet fullBleed()}
  <div class="min-h-screen bg-crust p-6">
    <div class="max-w-lg">
      <Card title="Recent activity" padding={false}>
        {#each ['thor staged v2026.8.2', 'laptop activated v2026.8.2', 'desktop came back online'] as line, i}
          <div class="px-4 py-2.5 text-sm text-subtitle {i > 0 ? 'border-t border-overlay0' : ''}">
            {line}
          </div>
        {/each}
      </Card>
    </div>
  </div>
{/snippet}

<Story name="Full Bleed" template={fullBleed} />

<!-- The resilience layout: two cards in the lg two-column grid. -->
{#snippet twoUp()}
  <div class="min-h-screen bg-crust p-6">
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <Card title="Data Replication">
        {#snippet headerRight()}
          <span class="text-xs font-mono">
            <span class="text-subtitle">worst block tolerates</span>
            <span class="text-mauve">2</span>
            <span class="text-subtitle">node failures</span>
          </span>
        {/snippet}
        <div class="h-40 grid place-items-center text-muted text-sm border border-dashed border-overlay0 rounded">
          storage panel
        </div>
      </Card>
      <Card title="State Machine Replication">
        {#snippet headerRight()}
          <span class="text-lg font-semibold text-green">Healthy</span>
        {/snippet}
        <div class="h-40 grid place-items-center text-muted text-sm border border-dashed border-overlay0 rounded">
          consensus panel
        </div>
      </Card>
    </div>
  </div>
{/snippet}

<Story name="Two-Up Grid (Resilience)" template={twoUp} />

<!-- Long title: truncation instead of pushing the right side out of the card. -->
{#snippet overflow()}
  <div class="min-h-screen bg-crust p-6">
    <div class="max-w-sm">
      <Card
        title="A very long section title that would otherwise collide with the status"
        subtitle="The title truncates; the right side keeps its space."
      >
        {#snippet headerRight()}
          <span class="text-lg font-semibold text-yellow">Degraded</span>
        {/snippet}
        <p class="text-sm text-subtitle">Body content.</p>
      </Card>
    </div>
  </div>
{/snippet}

<Story name="Overflowing Title" template={overflow} />
