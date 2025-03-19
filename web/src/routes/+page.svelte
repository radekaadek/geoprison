<script lang="ts">
  import { onMount } from 'svelte';
  import { browser } from '$app/environment';

  async function loadMap() {
    // Import Leaflet only on the client side
    const L = await import('leaflet');
    
    // Create map after the component is mounted
    let map = L.map('map').setView([52, 20], 7);
          
    let osmTopoLayer = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    })
    let cartoDarkLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      subdomains: 'abcd',
      maxZoom: 20
    })

    // Add the default layer to the map
    osmTopoLayer.addTo(map);

    let tileLayers = {
      "OpenStreetMap": osmTopoLayer,
      "CartoDark": cartoDarkLayer
    }

    let layerControl = L.control.layers(tileLayers).addTo(map);
  }
  
  onMount(async () => {
    if (browser) {
      loadMap();
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
