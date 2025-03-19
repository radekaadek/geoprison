<script lang="ts">
  import { onMount } from 'svelte';
  import { browser } from '$app/environment';

  function loadMap(L: typeof import("leaflet")) {
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
    cartoDarkLayer.addTo(map);

    let tileLayers = {
      "OpenStreetMap": osmTopoLayer,
      "CartoDark": cartoDarkLayer
    }

    let layerControl = L.control.layers(tileLayers).addTo(map);

    return map
  }

  async function runApp() {
    // Import Leaflet only on the client side
    const L = await import('leaflet');
    let map = loadMap(L);

    let myMarker: L.Marker
    map.on('mousedown', function(e) {
      let lat = e.latlng.lat
      let lon = e.latlng.lng
      myMarker = L.marker([lat, lon]).addTo(map)
        .bindPopup(`Lat: ${lat}, Lon: ${lon}`)
        .openPopup();
      })
    map.on('mouseup', function(e) {
      if (myMarker) {
        myMarker.remove()
      }
    })
  }
  
  onMount(async () => {
    if (browser) {
      runApp();
    }
  });
</script>


<div id="map"></div>


<style>
  #map {
    height: 100vh;
    width: 100vw;
  }
</style>

<svelte:head>
  {#if browser}
     <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
     integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
     crossorigin=""/>
  {/if}
</svelte:head>

