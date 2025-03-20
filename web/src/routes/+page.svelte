<script lang="ts">
  import { onMount } from 'svelte';
  import { browser } from '$app/environment';
  import { cellToBoundary, polygonToCells } from 'h3-js';

  let selectedPolygonGeoJSON = null
  let drawnItems: L.FeatureGroup
  let layerControl: L.Control.Layers
  let map: L.Map
  let hexagonLayer: L.Layer | null = null
  let idToPolygon: Map<string, L.Polygon> = new Map()

  type leafletType = typeof import("leaflet")
  type leafletDrawType = typeof import("leaflet-draw")

  function removeHexagons() {
    if (hexagonLayer) {
      idToPolygon = new Map()
      layerControl.removeLayer(hexagonLayer)
      drawnItems.removeFrom(map)
      hexagonLayer.remove()
      hexagonLayer = null
    }
  }

  // poly is a polygon geojson
  function editPolygon(poly: any, L: leafletType) {
    selectedPolygonGeoJSON = poly
    if (!selectedPolygonGeoJSON) {
      if (hexagonLayer) {
        removeHexagons()
      }
      return
    }
    const polygon = selectedPolygonGeoJSON['geometry']['coordinates'][0]
    const zoom = map.getZoom()
    const hexagons = polygonToCells(polygon, zoom-3)
    //[formatAsGeoJson] 	boolean 	Whether to provide GeoJSON output: [lng, lat], closed loops
    const boundaries = hexagons.map(c => cellToBoundary(c, true))
    const polygons = boundaries.map(b => L.polygon(b, {color: 'red', opacity: 0.5}))
    idToPolygon = new Map(hexagons.map((hex, i) => [hex, polygons[i]]));
    if (hexagonLayer) {
      removeHexagons()
    }
    const layer = L.layerGroup(idToPolygon.values().toArray()).addTo(drawnItems).addTo(map)
    hexagonLayer = layer
    layerControl.addOverlay(layer, "Hexagons").addTo(map)
  }

  async function loadMap(L: leafletType, leafletDraw: leafletDrawType) {
    // Create map after the component is mounted
    map = L.map('map').setView([52, 20], 7);
          
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

    layerControl = L.control.layers(tileLayers).addTo(map)

    // FeatureGroup to store editable layers
    drawnItems = new L.FeatureGroup().addTo(map)
    //map.addLayer(drawnItems);
    layerControl.addOverlay(drawnItems, "Polygon")

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
      editPolygon(null, L)
    });

    map.on('draw:created', function (event) {
      const layer = event.layer;  // Get the drawn polygon layer
      drawnItems.addLayer(layer); // Add it to the feature group

      const geojson = layer.toGeoJSON(); // Convert to GeoJSON
      editPolygon(geojson, L)
    });

    map.on('draw:saved', function (event) {
      const layer = event.layer;
      const geojson = layer.toGeoJSON();
      editPolygon(geojson, L)
    });



    return map
  }

  async function runApp() {
    // Import Leaflet only on the client side
    const L = await import('leaflet');
    const leafletDraw = await import('leaflet-draw');
    const map = loadMap(L, leafletDraw)
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

