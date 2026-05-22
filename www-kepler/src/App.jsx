import React, {useEffect, useRef, useState} from 'react';
import {useDispatch, useStore} from 'react-redux';
import KeplerGl from '@kepler.gl/components';
import {addDataToMap, removeDataset, wrapTo} from '@kepler.gl/actions';
import KeplerGlSchema from '@kepler.gl/schemas';
import LZString from 'lz-string';

import {EXPERIMENTS} from './experiments/index.js';

// Read the public Mapbox token from build-time env (Vite inlines it). With a
// token, Kepler stops nagging about "Mapbox Token not valid" and the few
// Kepler features that still call into the Mapbox SDK (geocoder, etc.) work.
// Basemap rendering still flows through maplibre-gl via the resolve.alias.
const MAP_ID = 'main';
const MAPBOX_TOKEN = import.meta.env.VITE_MAPBOX_ACCESS_TOKEN || '';

const MAP_STYLES = [
  {
    id: 'positron',
    label: 'Positron (light)',
    url: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
  },
  {
    id: 'dark-matter',
    label: 'Dark Matter',
    url: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  },
];

// URL hash schema: #s=<lz-compressed JSON of {ids, cfg}>
//   ids — array of selected EXPERIMENTS ids (drives which datasets to mount)
//   cfg — full kepler config from KeplerGlSchema.getConfigToSave(), holds
//         per-layer styling, mapState (view), mapStyle (basemap), filters, etc.
// Compressed because cfg is 5-15kB JSON; lz-string brings the hash to ~1-3kB.
function readUrlState() {
  if (typeof window === 'undefined') return null;
  const m = window.location.hash.match(/[#&]s=([^&]+)/);
  if (!m) return null;
  try {
    const json = LZString.decompressFromEncodedURIComponent(m[1]);
    if (!json) return null;
    const parsed = JSON.parse(json);
    if (!Array.isArray(parsed.ids)) return null;
    return parsed;
  } catch {
    return null;
  }
}

function writeUrlState(ids, cfg) {
  const payload = JSON.stringify({ids, cfg});
  const encoded = LZString.compressToEncodedURIComponent(payload);
  // replaceState avoids polluting browser history on every map pan/zoom.
  history.replaceState(null, '', '#s=' + encoded);
}

function buildMergedConfig(exps) {
  if (exps.length === 0) {
    return {version: 'v1', config: {mapStyle: {styleType: 'dark-matter'}}};
  }
  const layers = exps.flatMap((e) => e.config.config.visState.layers);
  return {
    version: 'v1',
    config: {
      visState: {layers},
      // First selected experiment's mapState seeds the initial view; once the
      // user pans/zooms, the redux subscriber persists the new mapState to URL.
      mapState: exps[0].config.config.mapState,
      mapStyle: {styleType: 'dark-matter'},
    },
  };
}

export default function App() {
  const dispatch = useDispatch();
  const store = useStore();
  const containerRef = useRef(null);
  const [size, setSize] = useState({w: window.innerWidth, h: window.innerHeight});

  const initialState = useRef(readUrlState());
  const [selectedIds, setSelectedIds] = useState(
    () => initialState.current?.ids ?? [EXPERIMENTS[0].id]
  );
  const selectedIdsRef = useRef(selectedIds);
  useEffect(() => { selectedIdsRef.current = selectedIds; }, [selectedIds]);

  useEffect(() => {
    const onResize = () => setSize({w: window.innerWidth, h: window.innerHeight});
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, []);

  // One-shot initial load: seed kepler with the experiments named in URL (or
  // the default first one) and apply the saved config if present so refresh
  // restores both selection and styling.
  useEffect(() => {
    const ids = selectedIdsRef.current;
    const exps = EXPERIMENTS.filter((e) => ids.includes(e.id));
    if (exps.length === 0) return;
    const datasets = exps.flatMap((e) => e.datasets);
    const config = initialState.current?.cfg ?? buildMergedConfig(exps);
    dispatch(
      wrapTo(
        MAP_ID,
        addDataToMap({
          datasets,
          options: {centerMap: false, readOnly: false, keepExistingConfig: false},
          config,
        })
      )
    );
    // Run once on mount — toggling happens via toggleExperiment below, which
    // does surgical add/remove instead of full rebuild to preserve styling.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Redux subscriber: serialize kepler state → URL hash on any state change,
  // debounced so a panning gesture doesn't fire dozens of replaceState calls.
  useEffect(() => {
    let timer;
    const unsub = store.subscribe(() => {
      clearTimeout(timer);
      timer = setTimeout(() => {
        const kepler = store.getState().keplerGl?.[MAP_ID];
        if (!kepler) return;
        const cfg = KeplerGlSchema.getConfigToSave(kepler);
        writeUrlState(selectedIdsRef.current, cfg);
      }, 500);
    });
    return () => { unsub(); clearTimeout(timer); };
  }, [store]);

  function toggleExperiment(id) {
    const exp = EXPERIMENTS.find((e) => e.id === id);
    if (!exp) return;
    const isOn = selectedIds.includes(id);
    if (isOn) {
      exp.datasets.forEach((d) =>
        dispatch(wrapTo(MAP_ID, removeDataset(d.info.id)))
      );
      setSelectedIds(selectedIds.filter((x) => x !== id));
    } else {
      // keepExistingConfig: true so already-mounted layers + user styling stay.
      dispatch(
        wrapTo(
          MAP_ID,
          addDataToMap({
            datasets: exp.datasets,
            options: {centerMap: false, readOnly: false, keepExistingConfig: true},
            config: exp.config,
          })
        )
      );
      setSelectedIds([...selectedIds, id]);
    }
  }

  const SIDEBAR_W = 240;

  return (
    <div ref={containerRef} style={{display: 'flex', height: '100%', width: '100%'}}>
      <aside style={{
        width: SIDEBAR_W,
        padding: '16px',
        background: '#161a22',
        borderRight: '1px solid #222',
        overflowY: 'auto',
      }}>
        <h2 style={{margin: '0 0 12px', fontSize: 14, letterSpacing: 0.4, color: '#8aa'}}>
          experiments
        </h2>
        <ul style={{listStyle: 'none', padding: 0, margin: 0}}>
          {EXPERIMENTS.map((e) => {
            const on = selectedIds.includes(e.id);
            return (
              <li key={e.id}>
                <label
                  style={{
                    display: 'flex',
                    alignItems: 'flex-start',
                    gap: 8,
                    width: '100%',
                    padding: '8px 10px',
                    margin: '2px 0',
                    background: on ? '#2a3140' : 'transparent',
                    color: '#eaeaea',
                    border: '1px solid ' + (on ? '#3d4a63' : 'transparent'),
                    borderRadius: 4,
                    cursor: 'pointer',
                    fontSize: 13,
                  }}
                >
                  <input
                    type="checkbox"
                    checked={on}
                    onChange={() => toggleExperiment(e.id)}
                    style={{marginTop: 2}}
                  />
                  <span style={{flex: 1}}>
                    {e.label}
                    {e.note && (
                      <div style={{fontSize: 11, color: '#778', marginTop: 2}}>{e.note}</div>
                    )}
                  </span>
                </label>
              </li>
            );
          })}
        </ul>
        <p style={{fontSize: 11, color: '#556', marginTop: 24, lineHeight: 1.5}}>
          Tiles served by pg_tileserv against overturemaps-pg.
          View + styling persist in the URL hash — copy it to share or
          bookmark a specific configuration.
        </p>
      </aside>
      <main style={{flex: 1, position: 'relative'}}>
        <KeplerGl
          id={MAP_ID}
          mapboxApiAccessToken={MAPBOX_TOKEN}
          mapStyles={MAP_STYLES}
          width={size.w - SIDEBAR_W}
          height={size.h}
        />
      </main>
    </div>
  );
}
