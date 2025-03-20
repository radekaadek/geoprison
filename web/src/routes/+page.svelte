<script lang="ts">
  import { onMount } from 'svelte';
  import { browser } from '$app/environment';

  async function loadMap(L: typeof import("leaflet")) {
    // Create map after the component is mounted
    const leafletDraw = await import('leaflet-draw');
    const map = L.map('map').setView([52, 20], 7);
          
    const osmTopoLayer = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    })
    const cartoDarkLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      subdomains: 'abcd',
      maxZoom: 20
    })

    // Add the default layer to the map
    cartoDarkLayer.addTo(map);

    const tileLayers = {
      "OpenStreetMap": osmTopoLayer,
      "CartoDark": cartoDarkLayer
    }

    const layerControl = L.control.layers(tileLayers).addTo(map)

    let selectedPolygonGeoJSON = null

    // FeatureGroup to store editable layers
    const drawnItems = new L.FeatureGroup();
    map.addLayer(drawnItems);

    const drawControl = new L.Control.Draw({
        edit: {
            featureGroup: drawnItems,
            edit: {
                poly: {
                    allowIntersection: false, // doesn't work here
              }
            }
        },
        draw: {
            polyline: false,  // Disable polylines
            marker: false,    // Disable markers
            circle: false,    // Disable circles
            circlemarker: false, // Disable circle markers
            rectangle: false, // Disable rectangles
            polygon: {
              allowIntersection: false, // Restricts shapes to simple polygons
            }
        }
    });

    map.addControl(drawControl);

    map.on('draw:drawstart', function (_) {
      drawnItems.clearLayers();
      selectedPolygonGeoJSON = null
    });

    map.on('draw:created', function (event) {
      const layer = event.layer;  // Get the drawn polygon layer
      drawnItems.addLayer(layer); // Add it to the feature group

      const geojson = layer.toGeoJSON(); // Convert to GeoJSON
      selectedPolygonGeoJSON = geojson
      console.log("CreaCreated polygon:", geojson)
    });

    map.on('draw:saved', function (event) {
      const layer = event.layer;
      const geojson = layer.toGeoJSON();
      selectedPolygonGeoJSON = geojson
    });


    return map
  }

  async function runApp() {
    // Import Leaflet only on the client side
    const L = await import('leaflet');
    const map = loadMap(L);
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
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet.draw/0.4.2/leaflet.draw.css"/>
  {/if}
</svelte:head>

