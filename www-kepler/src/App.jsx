import React, {useEffect, useRef, useState} from 'react';
import {useDispatch} from 'react-redux';
import KeplerGl from '@kepler.gl/components';
import {addDataToMap, wrapTo} from '@kepler.gl/actions';

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

export default function App() {
  const dispatch = useDispatch();
  const containerRef = useRef(null);
  const [size, setSize] = useState({w: window.innerWidth, h: window.innerHeight});
  const [active, setActive] = useState(EXPERIMENTS[0]?.id);

  useEffect(() => {
    const onResize = () => setSize({w: window.innerWidth, h: window.innerHeight});
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, []);

  useEffect(() => {
    const exp = EXPERIMENTS.find((e) => e.id === active);
    if (!exp) return;
    dispatch(
      wrapTo(
        MAP_ID,
        addDataToMap({
          datasets: exp.datasets ?? [],
          // centerMap fits-to-bounds using the tilejson, which for global
          // datasets zooms the map out below the layer's minzoom and
          // nothing ever renders. Per-experiment mapState in the config
          // is the source of truth instead.
          options: {centerMap: false, readOnly: false, keepExistingConfig: false},
          config: exp.config,
        })
      )
    );
  }, [active, dispatch]);

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
          {EXPERIMENTS.map((e) => (
            <li key={e.id}>
              <button
                onClick={() => setActive(e.id)}
                style={{
                  display: 'block',
                  width: '100%',
                  textAlign: 'left',
                  padding: '8px 10px',
                  margin: '2px 0',
                  background: e.id === active ? '#2a3140' : 'transparent',
                  color: '#eaeaea',
                  border: '1px solid ' + (e.id === active ? '#3d4a63' : 'transparent'),
                  borderRadius: 4,
                  cursor: 'pointer',
                  fontSize: 13,
                }}
              >
                {e.label}
                {e.note && (
                  <div style={{fontSize: 11, color: '#778', marginTop: 2}}>{e.note}</div>
                )}
              </button>
            </li>
          ))}
        </ul>
        <p style={{fontSize: 11, color: '#556', marginTop: 24, lineHeight: 1.5}}>
          Tiles served by pg_tileserv against overturemaps-pg.
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
