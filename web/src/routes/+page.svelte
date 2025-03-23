<script lang="ts">
  import { onMount } from 'svelte';
  import { browser } from '$app/environment';
  import { cellToBoundary, cellToLatLng, latLngToCell, polygonToCells } from 'h3-js';

  let selectedPolygonGeoJSON = null
  let layerControl: L.Control.Layers
  let map: L.Map
  let idToPolygon: Map<string, L.Polygon> = new Map()
  let idToStrategy: Map<string, string> = new Map()
  let polygonLayer: L.Layer | null = null
  let hexagonLayer: L.Layer | null = null
  let showStartGameButton = false

  const polygonLayerName = "Polygon"

  const strategy_to_color: Map<string, string> = new Map([
    ["Tit-for-tat", "blue"],
    ["Random", "black"],
    ["Harrington", "red"],
    ["Tester", "yellow"],
    ["Suspicious tit-for-tat", "purple"],
    ["Forgiving tit-for-tat", "green"],
  ])

  type leafletType = typeof import("leaflet")

  // removes all layers except the basemap
  function removeAllLayers() {
    for(; Object.keys(map._layers).length > 1;) {
      map.removeLayer(map._layers[Object.keys(map._layers)[1]]);
    }
  }

  $: if (polygonLayer) {
    layerControl.addOverlay(polygonLayer, polygonLayerName)
  }

  function removeHexagons() {
    if (hexagonLayer) {
      layerControl.removeLayer(hexagonLayer)
      map.removeLayer(hexagonLayer)
    }
    idToPolygon = new Map()
    idToStrategy = new Map()
  }

  const strategyId = 'strategy'
  const polygonPopupContent = `
    <select id='${strategyId}'>
      ${[...strategy_to_color.keys()]
        .map(strategy => `<option value='${strategy}'>${strategy}</option>`)
        .join("")}
    </select>
  `

  const onPolygonClick = (L: leafletType, e: L.LeafletMouseEvent) => {
    const previousPopup = document.getElementById("strategy")
    if (previousPopup) {
      previousPopup.remove()
    }
    const hex = latLngToCell(e.latlng.lng, e.latlng.lat, map.getZoom()-3)
    const polygon = idToPolygon.get(hex)
    const popup = L.popup()
    const [lat, lng] = cellToLatLng(hex);
    popup.setLatLng(L.latLng(lng, lat));
    popup.setContent(polygonPopupContent)
    popup.openOn(map)
    const select = document.getElementById(strategyId) as HTMLSelectElement | null
    if (!select) {
      return
    }
    select.addEventListener("change", (e) => {
      const strategy = select.value
      polygon?.setStyle({ color: strategy_to_color.get(strategy) })
      idToStrategy.set(hex, strategy)
    })
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
    const hexagons = polygonToCells(polygon, zoom-3)
    const boundaries = hexagons.map(c => cellToBoundary(c, true))
    idToStrategy = new Map(hexagons.map((hex, i) => [hex, "Tit-for-tat"]));
    const color = strategy_to_color.get("Tit-for-tat")
    const polygons = boundaries.map(b => L.polygon(b, {
      color: color,
      opacity: 0.5,
    }).on("click", (e) => onPolygonClick(L, e)))
    idToPolygon = new Map(hexagons.map((hex, i) => [hex, polygons[i]]));
    hexagonLayer = L.layerGroup(Array.from(idToPolygon.values()))
    layerControl.addOverlay(hexagonLayer, "Hexagons")
    hexagonLayer.addTo(map)
    showStartGameButton = true
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
      removalMode: false,
      editMode: false,
      dragMode: false,
      cutPolygon: false
    });

    map.on("pm:create", (event) => {
      removeAllLayers()

      if (polygonLayer)
        layerControl.removeLayer(polygonLayer);
      polygonLayer = event.layer
      const fg = new L.FeatureGroup()
      fg.addLayer(event.layer)
      const polygonGeoJSON = fg.toGeoJSON()
      editPolygon(polygonGeoJSON, L)
    })

    map.on("pm:edit", (event) => {
      removeAllLayers()
      const fg = new L.FeatureGroup()
      fg.addLayer(event.layer)
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

  async function startGame() {
    console.log("start game")
  }
  
  onMount(async () => {
    if (browser) {
      runApp();
    }
  });
</script>


<div id="map">
</div>
{#if showStartGameButton}
  <button on:click={startGame}>Start Game</button>
{/if}


<style>
  #map {
    height: 100vh;
    width: 100vw;
  }
  button {
    background-color: red;
    color: white;
    font-size: 18px; /* Smaller font for responsiveness */
    padding: 12px 24px; /* Adjusted padding */
    border: none;
    border-radius: 8px;
    cursor: pointer;
    position: absolute;
    top: 20px; /* Move it to the top */
    left: 50%;
    transform: translateX(-50%);
    z-index: 9999; /* Ensure it's above Leaflet */
    white-space: nowrap; /* Prevent text wrapping */
  }

  /* Make it more responsive */
  @media (max-width: 600px) {
    button {
      font-size: 16px;
      padding: 10px 20px;
    }
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

