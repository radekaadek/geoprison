<script lang="ts">
  import { onMount } from 'svelte';
  import { browser } from '$app/environment';
  
  let map;
  
  onMount(async () => {
    if (browser) {
      // Import Leaflet only on the client side
      const L = await import('leaflet');
      
      // Create map after the component is mounted
      map = L.map('map').setView([52, 20], 7);
      
      L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
      }).addTo(map);
    }
  });
</script>

<div id="map"></div>

<svelte:head>
  {#if browser}
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.6.0/dist/leaflet.css" />
  {/if}
</svelte:head>

<style>
  #map {
    height: 100vh;
    width: 100vw;
  }
</style>
