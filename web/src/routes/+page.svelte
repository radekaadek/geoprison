<script lang="ts">
  import { onMount } from 'svelte';
  import { browser } from '$app/environment';
  import { cellToBoundary, polygonToCells } from 'h3-js';

  let selectedPolygonGeoJSON = null
  let layerControl: L.Control.Layers
  let map: L.Map
  let idToPolygon: Map<string, L.Polygon> = new Map()
  let polygonLayer: L.Layer | null = null
  let hexagonLayer: L.Layer | null = null

  type leafletType = typeof import("leaflet")

  // removes all layers except the basemap
  function removeAllLayers() {
    for(; Object.keys(map._layers).length > 1;) {
      map.removeLayer(map._layers[Object.keys(map._layers)[1]]);
    }
  }

  $: if (polygonLayer) {
    layerControl.addOverlay(polygonLayer, "Polygon")
    polygonLayer.addTo(map)
  }

  $: if (idToPolygon) {
  }


  function removeHexagons() {
    if (hexagonLayer) {
      layerControl.removeLayer(hexagonLayer)
      map.removeLayer(hexagonLayer)
    }
    idToPolygon = new Map()
  }

  // poly is a polygon geojson
  function editPolygon(poly: any, L: leafletType) {
    removeHexagons()
    selectedPolygonGeoJSON = poly
    if (!selectedPolygonGeoJSON) {
      return
    }
    const zoom = map.getZoom()
    const polygon = poly.features[0].geometry.coordinates
    console.log(polygon)
    const hexagons = polygonToCells(polygon, zoom-3)
    const boundaries = hexagons.map(c => cellToBoundary(c, true))
    const polygons = boundaries.map(b => L.polygon(b, {color: 'red', opacity: 0.5}))
    idToPolygon = new Map(hexagons.map((hex, i) => [hex, polygons[i]]));
    hexagonLayer = L.layerGroup(Array.from(idToPolygon.values()))
    layerControl.addOverlay(hexagonLayer, "Hexagons")
    hexagonLayer.addTo(map)
  }

  async function loadMap(L: leafletType) {
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
    map.pm.addControls({  
      position: 'topleft',  
      drawCircleMarker: false,
      rotateMode: false,
      drawCircle: false,
      drawPolyline: false,
      drawText: false,
      drawMarker: false,
    });

    map.on("pm:create", (shape) => {
      removeAllLayers()

      if (polygonLayer)
        layerControl.removeLayer(polygonLayer);
      polygonLayer = shape.layer
      const fg = new L.FeatureGroup()
      fg.addLayer(shape.layer)
      const polygonGeoJSON = fg.toGeoJSON()
      editPolygon(polygonGeoJSON, L)
    })

    return map
  }
  async function loadLeafletModules() {
    const [L, leafletCSS, geoman] = await Promise.all([
        import('leaflet'),
        import('leaflet/dist/leaflet.css'),
        import('@geoman-io/leaflet-geoman-free')
    ]);

    return { L, leafletCSS, geoman };
  }
  async function runApp() {
    loadLeafletModules().then(({ L }) => {
        loadMap(L)
    });

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
    <link
      rel="stylesheet"
      href="https://unpkg.com/@geoman-io/leaflet-geoman-free@latest/dist/leaflet-geoman.css"
    />
  {/if}
</svelte:head>

