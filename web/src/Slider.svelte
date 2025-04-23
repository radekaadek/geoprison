<script lang="ts">
  import type { Map } from 'leaflet'; // Import the type for the map
  export let label: string;
  export let min: number;
  export let max: number;
  export let step: number;
  export let value: number; // Explicitly type 'value' as number
  export let map: Map | undefined; // Explicitly type 'map' as Map or undefined

  function handleMouseEnter() {
    if (map && map.dragging) {
      map.dragging.disable();
    }
  }

  function handleMouseLeave() {
    if (map && map.dragging) {
      map.dragging.enable();
    }
  }
</script>

<div class="slider">
  <div class="sliderValue">{label}: {value}</div>
  <input
    type="range"
    {min}
    {max}
    {step}
    bind:value
    on:mouseenter={handleMouseEnter}
    on:mouseleave={handleMouseLeave}
  />
  <input class="sliderField" type="number" {min} {max} bind:value />
</div>

<style>
  .slider {
    text-align: center;
    justify-content: center;
    background-color: #add8e6;
    border: 1px solid #6abf69; /* Slightly darker green for contrast */
    display: flex;
    flex-direction: column;
  }
  .sliderValue {
    padding-right: 0.5rem;
    padding-left: 0.5rem;
    padding-top: 0.3rem;
    padding-bottom: 0.3rem;
  }

  .sliderField {
    text-align: center;
    justify-content: center;
    padding: 0.3rem;
    border: 3px solid #6abf69; /* Slightly darker green for contrast */
  }

  input[type="range"] {
    position: relative;
    z-index: 1000;
    pointer-events: auto;
  }
  @media (max-width: 480px) {
    input[type="range"] {
      width: 90%;
      font-size: 1.1rem;
    }
  }

</style>


