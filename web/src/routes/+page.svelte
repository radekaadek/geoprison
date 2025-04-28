<script lang="ts">
  import { onMount } from 'svelte';
  import { browser } from '$app/environment';
  import { cellToBoundary, cellToLatLng, latLngToCell, polygonToCells } from 'h3-js';
  import Slider from "../Slider.svelte";

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
  let noise = 0
  let numberOfRounds = 15
  let numberOfSteps = 15
  let currentStep = 0
  // Whether the strategies have been loaded from the server
  let loadedStrategies = false

  // Axelrod payoff matrix
  let r = 3
  let s = 0
  let t = 5
  let p = 1

  const polygonLayerName = "Polygon"
  const hexagonLayerName = "Hexagons"

  $: handleStepChange(currentStep)

  const handleStepChange = (step: number) => {
    if (step < gameStates.length) {
      updateGameHexagons(gameStates[currentStep], idToPolygon)
    }
  }

  type stratAndScore = {
    strategy: string,
    score: number
  }
  let gameStates: Array<Map<string, stratAndScore>> = []

  let defaultStrategy = "Tester"
  const strategy_to_color: Map<string, string> = new Map([
  //   ["Tester", "yellow"],
  //   ["Tit-for-tat", "blue"],
  //   ["Random", "black"],
  //   ["Harrington", "red"],
  //   ["Suspicious tit-for-tat", "purple"],
  //   ["Forgiving tit-for-tat", "brown"],
  //   ["Grudger", "orange"],
  //   ["Cooperator", "green"],
  //   ["Defector", "pink"],
  //   ["Alternator", "gray"],
  ])
  const strategyToID: Map<string, number> = new Map(Object.entries({
  //   "Alternator": 0,
  //   "Cooperator": 1,
  //   "Defector": 2,
  //   "Forgiving tit-for-tat": 3,
  //   "Grudger": 4,
  //   "Harrington": 5,
  //   "Random": 6,
  //   "Suspicious tit-for-tat": 7,
  //   "Tester": 8,
  //   "Tit-for-tat": 9,
  }));

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

  const strategyDivId = 'strategy'
  const getPolygonPopupContent = (hexID: string) => {
    return `
    <select id='${strategyDivId}'>
      <option value='select'>Select a strategy</option>
      ${[...strategy_to_color.keys()]
        .map(strategy => `<option value='${strategy}'>${strategy}</option>`)
        .join("")}
    </select>
  `}

  const getPolygonScorePopupContent = (hexID: string, score: number, strategy: string) => {
    return `
    <div id='${hexID}Score'>
      <p>Current Strategy: ${strategy}</p>
      <p>Previous Score: ${score}</p>
      <p>Hex ID: ${hexID}</p>
    </div>
  `}

  const getPolygonPopup = (hex: string) => {
    const popup = L.popup()
    const [lat, lng] = cellToLatLng(hex);
    popup.setLatLng(L.latLng(lng, lat));
    popup.setContent(getPolygonPopupContent(hex))
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
    const select = document.getElementById(strategyDivId) as HTMLSelectElement | null
    if (!select) {
      alert("Something went wrong, could not find select element")
      return
    }
    select.addEventListener("change", (e) => {
      const strategy = select.value
      if (strategy === 'select') {
        return
      }
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

  async function loadMap(strategyPromise: Promise<Response>) {
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

    // Before adding the controls ask the server for the strategies
    let strategiesResponse: Map<string, string[]> = await strategyPromise
    .catch(error => {
      console.error('Error:', error);
      const msg = `The server at ${serverURL} is not running. Please start the server before starting the game.`;
      alert(msg);
      return
    })
    .then(async response => {
      if (!response || !response.ok) {
        const msg = `The server at ${serverURL} is not running. Please start the server before starting the game.`;
        alert(msg);
        return
      }
      return await response.json();
    })

    for (const [id, strategy] of Object.entries(strategiesResponse)) {
      strategy_to_color.set(strategy[0], strategy[1])
      strategyToID.set(strategy[0], parseInt(id))
    }

    loadedStrategies = true


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
    const serverStatusPromise = fetch(`${serverURL}/`)
    const strategyPromise = fetch(`${serverURL}/strategies`)
    loadLeafletModules().then(async () => {
        await loadMap(strategyPromise)
    });
    // Check if the server is running
    const response = await serverStatusPromise
      .then(response => {
        if (!response.ok) {
          const msg = `The server at ${serverURL} is not running. Please start the server before starting the game.`;
          alert(msg);
        }
        return response.json();
        })
      .catch(error => {
        console.error('Error:', error);
        const msg = `The server at ${serverURL} is not running. Please start the server before starting the game.`;
        alert(msg);
        return
      })
  }

  async function game_step(idStrategies: Map<string, string>, numberOfRounds: number = 15, r: number = 4, s: number = 0, t: number = 5, p: number = 1) {
    const url = `${serverURL}/game_step?rounds=${numberOfRounds}&noise=${noise}&r=${r}&s=${s}&t=${t}&p=${p}`;
    // Convert the map to a dictionary with integer keys
    const idToStrategyID: Map<string, number> = new Map()
    idStrategies.forEach((strategy, id) => {
      const stratID = strategyToID.get(strategy)
      if (stratID === undefined) {
        console.error(`Unknown strategy: ${strategy}`)
        console.error(`Strategies: ${[...strategyToID.keys()]}`)
        return
      }
      idToStrategyID.set(id, stratID)
    })
    const obj = Object.fromEntries(idToStrategyID)
    const body = JSON.stringify(obj)
    return fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: body,
    })
  }

  async function startGame() {
    currentStep = 0
    showStartGameButton = false
    gameStarted = true
    map.closePopup();
    map.pm.addControls({
      drawPolygon: false,
      drawRectangle: false,
    })
      
    let idStrategies: Map<string, string> = idToStrategy
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
    idToStrategy = idStrategies
    const polyArray = Array.from(polygons)
    idToPolygon = new Map(hexArray.map((hex, i) => [hex, polyArray[i]]))
    hexagonLayer = L.layerGroup(Array.from(idToPolygon.values()))
    layerControl.addOverlay(hexagonLayer, hexagonLayerName)
    hexagonLayer.addTo(map)

    // Add the starting strategies to the results
    const startingStrategies: Map<string, stratAndScore> = new Map()
    idToStrategy.forEach((strategy, hexID) => {
      const score = 0
      startingStrategies.set(hexID, {
        strategy: strategy,
        score: score
      })
    })
    gameStates.push(startingStrategies)
  
    for (let i = 1; i <= numberOfSteps; i++) {
      const gameResults = await game_step(idToStrategy, numberOfRounds, r, s, t, p)
        .then(response => {
          if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
          }
          return response.json();
        })
        .then(data => data)
        .catch(error => {
          console.error('Error:', error);
          alert(`Could not get game results from the server.\nPlease check that the server is running and try again.`)
          return
        })
      const result_strategies = gameResults.updated_strategies
      const result_scores = gameResults.scores
      const result_strategies_map: Map<string, string> = new Map(Object.entries(result_strategies))
      const currentState: Map<string, stratAndScore> = new Map()
      result_strategies_map.forEach((strategy, hexID) => {
        currentState.set(hexID, {
          strategy: strategy,
          score: result_scores[hexID]
        })
      })
      gameStates.push(currentState)
      idToStrategy = result_strategies_map

      currentStep = i
      updateGameHexagons(currentState, idToPolygon)

    }

  }

  // Update hexagons with the current game state
  const updateGameHexagons = (hexToStratNScore: Map<string, stratAndScore>, idToPolygon: Map<string, L.Polygon>) => {
    idToPolygon.forEach((polygon, hexID) => {
      const queryResult = hexToStratNScore.get(hexID) as stratAndScore
      const strat = queryResult.strategy
      const color = strategy_to_color.get(strat)
      polygon.setStyle({ color: color })
      // add a popup with the strategy name and score
      const score = queryResult.score
      if (score !== undefined) {
        // clear the previous popup
        polygon.unbindPopup()
        const popup = getPolygonScorePopupContent(hexID, score, strat)
        polygon.bindPopup(popup)
      }
    })
  }

  const randomizeHexStrategies = () => {
    const hexagons = idToPolygon.keys()
    const hexArray = Array.from(hexagons)
    const randomizedStrategies = new Map()
    const stratList: string[] = []
    // find the element with the id "strategiesToRandomize"
    const strategiesParentDiv: any = document.getElementById("strategiesToRandomize")
    // get the value of the checkboxes
    const checkboxes = strategiesParentDiv.querySelectorAll("input[type='checkbox']") as NodeListOf<HTMLInputElement>
    checkboxes.forEach((checkbox) => {
      if (checkbox.checked) {
        stratList.push(checkbox.id)
      }
    })
    hexArray.forEach((hID) => {
      const randomStrategy = stratList[Math.floor(Math.random() * stratList.length)]
      randomizedStrategies.set(hID, randomStrategy)
      const color = strategy_to_color.get(randomStrategy)
      const polygon = idToPolygon.get(hID)
      if (!polygon) {
        console.error(`Polygon for hexagon ${hID} not found`)
        return
      }
      polygon.setStyle({ color: color })
    })
    idToStrategy = randomizedStrategies
  }

  const dropdownMobileToggle = () => {
    if (typeof window.matchMedia !== 'undefined' && window.matchMedia('(hover: none)').matches) {
      const dropdown = document.getElementById("strategiesToRandomize");
      const dropbtn = document.getElementById("dropbtn");
      if (dropdown) {
        if (dropdown.style.display === "none") {
          dropdown.style.display = "block";
          // change color of dropdown button
          if (dropbtn) {
            dropbtn.style.backgroundColor = "#3e8e41";
          }
        } else {
          dropdown.style.display = "none";
          if (dropbtn) {
            dropbtn.style.backgroundColor = "red";
          }
        }
      }
    }
  }
  
  onMount(async () => {
      runApp();
  });
</script>


<div id="map">
</div>

{#if showStartGameButton}
  <button class="centerTop" on:click={() => startGame()}>Start Game</button>
{/if}


{#if !gameStarted}
  <div id="controls">
    <table>
            <tbody>
                <tr>
                    <td>
                        <div class="cell-content">
                            <span>CC/R</span>
                            <input type="number" id="payout_cc" name="payout_cc" bind:value={r}>
                        </div>
                    </td>
                    <td>
                        <div class="cell-content">
                            <span>CD/S</span>
                            <input type="number" id="payout_cd" name="payout_cd" bind:value={s}>
                        </div>
                    </td>
                </tr>
                <tr>
                    <td>
                        <div class="cell-content">
                            <span>DC/T</span>
                            <input type="number" id="payout_dc" name="payout_dc" bind:value={t}>
                        </div>
                    </td>
                    <td>
                        <div class="cell-content">
                            <span>DD/P</span>
                            <input type="number" id="payout_dd" name="payout_dd" bind:value={p}>
                        </div>
                    </td>
                </tr>
            </tbody>
        </table>
    <!-- <div id="defaultStrategy"> -->
    <!--   <div id="dsString">Default Strategy:</div> -->
    <!--   <select id="strategySelect" bind:value={defaultStrategy}> -->
    <!--     {#each [...strategy_to_color.keys()] as strategy} -->
    <!--       <option value={strategy}>{strategy}</option> -->
    <!--     {/each} -->
    <!--   </select> -->
    <!-- </div> -->
      <div class="dropdown">
        <button class="dropbtn" id="dropbtn" on:click={()=>dropdownMobileToggle()}>Strategies to Randomize</button>
        <div id="strategiesToRandomize" class="dropdown-content">
         {#if loadedStrategies}
          {#each [...strategy_to_color.keys()] as strategy}
            <div class="strategyCheckbox">
              <input type="checkbox" id={strategy}/>
              <label for={strategy}>{strategy}</label>
            </div>
          {/each}
         {/if}
        </div>
      </div> 
      <button on:click={() => randomizeHexStrategies()}>Randomize Strategies</button>
    <Slider label="Noise" min={0} max={1} step={0.1} bind:value={noise} {map} />
    <Slider label="Hex Level" min={0} max={15} step={1} bind:value={hexLevel} {map} />
    <Slider label="Rounds" min={1} max={50} step={1} bind:value={numberOfRounds} {map} />
    <Slider label="Number of Steps to Generate" min={1} max={200} step={1} bind:value={numberOfSteps} {map} />
  </div>
{:else}
  <div id="controls">
    <Slider label="Current Step" min={0} max={numberOfSteps} step={1} bind:value={currentStep} {map} />
  </div>
{/if}


<style>
  #map {
    height: 100vh;
    width: 100vw;
  }

  /* The container <div> - needed to position the dropdown content */
  .dropdown {
    position: relative;
    display: inline-block;
  }

  /* Dropdown Content (Hidden by Default) */
  .dropdown-content {
    display: none;
    position: absolute;
    left: 50%;
    transform: translateX(-50%);
    background-color: #f1f1f1;
    box-shadow: 0px 8px 16px 0px rgba(0,0,0,0.2);
    z-index: 1001;
    white-space: nowrap; /* Prevent text wrapping */
    padding: 0.2rem;
  }

  /* Show the dropdown menu on hover */
  /* On click for mobile in script */
  @media (any-hover: hover) {
    .dropdown:hover .dropdown-content {display: block;}
    .dropdown:hover .dropbtn {background-color: #3e8e41;} 
  }

  /* Change the background color of the dropdown button when the dropdown content is shown */
  .centerTop {
    cursor: pointer;
    position: absolute;
    top: 20px; /* Move it to the top */
    left: 50%;
    transform: translateX(-50%);
    z-index: 1000; /* Ensure it's above Leaflet */
  }

  button {
    background-color: red;
    width: 100%;
    color: white;
    font-size: 18px; /* Smaller font for responsiveness */
    padding: 12px 24px; /* Adjusted padding */
    border: none;
    border-radius: 8px;
    text-align: center;
  }

  /* Make it more responsive */
  @media (max-width: 600px) {
    button {
      font-size: 16px;
      padding: 10px 20px;
    }
  }

  #controls {
    position: absolute;
    left: 0%;
    top: 17%;
    width: 15%;
    /*transform: translateY(-50%);*/
    font-size: 1rem; /* Large text */
    padding: 0.2rem;
    z-index: 1000;
    margin: 0.2rem;
    display: flex;
    flex-direction: column;
    gap: 1rem;
    text-align: center;
    justify-content: center;
  }

  #defaultStrategy {
    background-color: #add8e6;
    border: 1px solid #6abf69; /* Slightly darker green for contrast */
  }

  #dsString {
    color: blue;
  }

  @media (max-width: 480px) {
      #controls {
          font-size: 1.1rem;
          padding: 0.8rem;
      }
  }

  @media (max-width: 480px) {
    #controls {
      width: 45%;
    }
    #strategySelect {
      width: 90%;
    }
  }

  /* Axelrod payoff matrix */
        table {
            border-collapse: collapse;
            border-radius: 8px; /* Equivalent to Tailwind rounded-md */
            overflow: hidden; /* Hide overflow for rounded corners */
            table-layout: fixed; /* Fix table layout for better column width control */
            background-color: #f9fafb; /* Equivalent to Tailwind bg-gray-50 */
        }

        .cell-content {
            display: flex;
            flex-direction: column;
            align-items: center;
        }

        .cell-content span {
            font-size: 14px; /* Equivalent to Tailwind text-sm */
            color: #374151; /* Equivalent to Tailwind text-gray-700 */
            margin-bottom: 0.15rem; /* Equivalent to Tailwind mb-1 */
        }

        input[type="number"] {
            width: 75%;
            padding: 0.3rem;
            border: 1px solid #d1d5db; /* Equivalent to Tailwind gray-300 */
            border-radius: 6px; /* Equivalent to Tailwind rounded-md */
            text-align: center;
            -moz-appearance: textfield; /* Hide default number input arrows in Firefox */
            appearance: textfield;
        }

        input[type="number"]::-webkit-outer-spin-button,
        input[type="number"]::-webkit-inner-spin-button {
            -webkit-appearance: none; /* Hide default number input arrows in Chrome, Safari, Edge */
            margin: 0;
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

