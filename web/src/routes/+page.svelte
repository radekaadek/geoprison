<script lang="ts">
  import { onMount } from 'svelte';
  import { browser } from '$app/environment';
  import { cellToBoundary, cellToLatLng, latLngToCell, polygonToCells } from 'h3-js';

  let serverURL = "http://localhost:8000"

  let selectedPolygonGeoJSON: any = null
  let layerControl: L.Control.Layers
  let map: L.Map
  let idToPolygon: Map<string, L.Polygon> = new Map()
  let idToStrategy: Map<string, string> = new Map()
  let polygonLayer: L.Layer | null = null
  let hexagonLayer: L.Layer | null = null
  let showStartGameButton = false
  let startingMapZoom = 7
  let hexLevel = zoomToHexSize(startingMapZoom)
  let gameStarted = false
  let L: leafletType

  const polygonLayerName = "Polygon"
  const hexagonLayerName = "Hexagons"

  let defaultStrategy = "Tit-for-tat"
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

  const getPolygonPopup = (hex: string) => {
    const popup = L.popup()
    const [lat, lng] = cellToLatLng(hex);
    popup.setLatLng(L.latLng(lng, lat));
    popup.setContent(polygonPopupContent)
    return popup
  }

  const onPolygonClick = (e: L.LeafletMouseEvent) => {
    const previousPopup = document.getElementById("strategy")
    if (previousPopup) {
      previousPopup.remove()
    }
    const hex = latLngToCell(e.latlng.lng, e.latlng.lat, hexLevel)
    const polygon = idToPolygon.get(hex)
    const popup = getPolygonPopup(hex)
    popup.openOn(map)
    const select = document.getElementById(strategyId) as HTMLSelectElement | null
    if (!select) {
      alert("Something went wrong, could not find select element")
      return
    }
    select.addEventListener("change", (e) => {
      const strategy = select.value
      polygon?.setStyle({ color: strategy_to_color.get(strategy) })
      idToStrategy.set(hex, strategy)
    })
  }

  // poly is a polygon geojson
  function editPolygon(poly: any) {
    removeHexagons()
    selectedPolygonGeoJSON = poly
    if (!selectedPolygonGeoJSON) {
      return
    }
    const polygon = poly.features[0].geometry.coordinates
    const hexagons = polygonToCells(polygon, hexLevel)
    const boundaries = hexagons.map(c => cellToBoundary(c, true))
    idToStrategy = new Map(hexagons.map((hex, i) => [hex, defaultStrategy]));
    const color = strategy_to_color.get(defaultStrategy)
    const polygons = boundaries.map(b => L.polygon(b, {
      color: color,
      opacity: 0.5,
    }).on("click", (e) => onPolygonClick(e)))
    idToPolygon = new Map(hexagons.map((hex, i) => [hex, polygons[i]]));
    hexagonLayer = L.layerGroup(Array.from(idToPolygon.values()))
    layerControl.addOverlay(hexagonLayer, hexagonLayerName)
    hexagonLayer.addTo(map)
    hexagons.length > 0 ? showStartGameButton = true : showStartGameButton = false
  }

  function zoomToHexSize(zoom: number): number {
    const zoomToHexLevel: Map<number, number> = new Map([
      [8, 5],
      [11, 7],
      [13, 8],
      [14, 9],
      [15, 10],
      [16, 10],
      [17, 11],
    ])
    return zoomToHexLevel.has(zoom) ? zoomToHexLevel.get(zoom) as number : Math.floor(zoom*0.6)
  }

  async function loadMap() {
    // Create map after the component is mounted
    map = L.map('map').setView([52, 20], startingMapZoom);
          
    const osmTopoLayer = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    })
    const cartoDarkLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      subdomains: 'abcd',
      maxZoom: 20,
      minZoom: 2
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

    const leafletContainer = document.querySelector(".leaflet-top.leaflet-left");
    const hexLevelDiv = document.getElementById("hexLevel");

    if (leafletContainer && hexLevelDiv) {
      leafletContainer.appendChild(hexLevelDiv);
    } else {
      console.error("Could not find leaflet container or hexLevelDiv");
    }

    map.on("zoomend", (e) => {
      hexLevel = zoomToHexSize(map.getZoom())
    });

    map.on("pm:create", (event) => {
      removeAllLayers()

      if (polygonLayer)
        layerControl.removeLayer(polygonLayer);
      polygonLayer = event.layer
      const fg = new L.FeatureGroup()
      fg.addLayer(event.layer)
      const polygonGeoJSON = fg.toGeoJSON()
      editPolygon(polygonGeoJSON)
    })

    map.on("pm:edit", (event) => {
      removeAllLayers()
      const fg = new L.FeatureGroup()
      fg.addLayer(event.layer)
      const polygonGeoJSON = fg.toGeoJSON()
      editPolygon(polygonGeoJSON)
    })

    return map
  }
  async function loadLeafletModules() {
    const [Leaflet, leafletCSS, geoman] = await Promise.all([
        import('leaflet'),
        import('leaflet/dist/leaflet.css'),
        import('@geoman-io/leaflet-geoman-free')
    ]);
    L = Leaflet

    return { L, leafletCSS, geoman };
  }
  async function runApp() {
    loadLeafletModules().then(() => {
        loadMap()
    });

  }

  async function startGame() {
    showStartGameButton = false
    gameStarted = true
    map.closePopup();
    map.pm.addControls({
      drawPolygon: false,
      drawRectangle: false,
    })
      
    console.log("start game")
    const idStrategies: Map<string, string> = idToStrategy
    removeHexagons()
    const hexagons = idStrategies.keys()
    const hexArray = Array.from(hexagons)
    const polygons = hexArray.map((hID) => {
      const boundary = cellToBoundary(hID, true)
      return L.polygon(boundary, {
        color: strategy_to_color.get(idStrategies.get(hID) as string),
        opacity: 0.5,
      })
    })
    const polyArray = Array.from(polygons)
    idToPolygon = new Map(hexArray.map((hex, i) => [hex, polyArray[i]]))
    hexagonLayer = L.layerGroup(Array.from(idToPolygon.values()))
    layerControl.addOverlay(hexagonLayer, hexagonLayerName)
    hexagonLayer.addTo(map)

    const numberOfRounds = 15;

    // Get game results by passing hexID to strategy to the api
    const hexIDList: Array<[string, string]> = [];
    idStrategies.forEach((strategy, hexID) => {
      hexIDList.push([hexID, strategy]);
    });

    const url = `${serverURL}/game?rounds=${numberOfRounds}`;

    const gameResults = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(
        idStrategies
      ),
    })
      .then(response => {
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.json();
      })
      .then(data => data)
      .catch(error => console.error('Error:', error));
  
    console.log(gameResults)

  }
  
  onMount(async () => {
      runApp();
  });
</script>


<div id="map">
</div>
{#if showStartGameButton}
  <button on:click={() => startGame()}>Start Game</button>
{/if}


{#if !gameStarted}
  <div id="hexLevel">
    <input type="range" min="0" max="15" step="1" bind:value={hexLevel}
       on:mouseenter={() => map.dragging.disable()} 
       on:mouseleave={() => map.dragging.enable()}>
  </div>
{/if}
{#if !gameStarted}
  <div id="defaultStrategy">
    <select bind:value={defaultStrategy}>
      {#each [...strategy_to_color.keys()] as strategy}
        <option value={strategy}>{strategy}</option>
      {/each}
    </select>
  </div>
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

  #defaultStrategy {
    position: absolute;
    left: 0%;
    top: 50%;
    transform: translateY(-50%);
    font-size: 1rem; /* Large text */
    padding: 0.2rem;
    z-index: 1000;
    background-color: #add8e6;
    border: 1px solid #6abf69; /* Slightly darker green for contrast */
  }

  /* Responsive Design */
  @media (max-width: 768px) {
      #defaultStrategy {
          left: 50%;
          top: auto;
          bottom: 10%;
          transform: translateX(-50%);
          font-size: 1.2rem; /* Larger font for mobile readability */
          width: 90%; /* Ensures it fits within smaller screens */
          text-align: center;
          padding: 0.6rem;
      }
  }

  @media (max-width: 480px) {
      #defaultStrategy {
          width: 95%;
          font-size: 1.1rem;
          padding: 0.8rem;
      }
  }

  input[type="range"] {
    position: relative;
    z-index: 1000;
    pointer-events: auto;
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

