// Kepler 3.x vector-tile datasets need:
//   info.type = 'vector-tile'
//   opts.metadata.remoteTileFormat = 'mvt'
//   opts.metadata.tilesetDataUrl     (absolute, with {z}/{x}/{y} placeholders)
//   opts.metadata.tilesetMetadataUrl (absolute, returns TileJSON 3.0 with vector_layers)
//
// Kepler's getTileUrl runs the URL through parseUri and rejects anything
// without a protocol+host, so we build absolute URLs at runtime from
// window.location.origin. Works for any deployment without baking the
// host into the bundle.
//
// The TileJSON files under public/tilejson/ are committed in the repo and
// served by the www-kepler container's nginx at /kepler/tilejson/*. They
// translate pg_tileserv's own metadata format into TileJSON + vector_layers,
// which is what loaders.gl/@kepler.gl/table expects.

function mvtDataset({id, label, tilesetDataPath, tilesetMetadataPath, color, mapState}) {
  const origin = typeof window !== 'undefined' ? window.location.origin : '';
  return {
    datasets: [
      {
        info: {id, label, type: 'vector-tile'},
        data: {fields: [], rows: []},
        // metadata at the top level — createNewDataEntry pulls everything
        // outside of {info, data} into an opts object, so this surfaces as
        // datasetInfo.opts.metadata where refreshVectorTileMetadata reads it.
        metadata: {
          remoteTileFormat: 'mvt',
          tilesetDataUrl: `${origin}${tilesetDataPath}`,
          tilesetMetadataUrl: `${origin}${tilesetMetadataPath}`,
        },
      },
    ],
    config: {
      version: 'v1',
      config: {
        visState: {
          layers: [
            {
              id: `${id}-vt`,
              type: 'vectorTile',
              config: {
                dataId: id,
                label,
                isVisible: true,
                visConfig: {strokeColor: color, fillColor: color, opacity: 0.7},
              },
            },
          ],
        },
        mapState,
        mapStyle: {styleType: 'voyager'},
      },
    },
  };
}

export const TABLES = [
  {
    id: 'all-places',
    label: 'All places (MVT)',
    note: '~72M points, z ≥ 8 (opens over SF)',
    ...mvtDataset({
      id: 'all-places',
      label: 'Overture places',
      tilesetDataPath: '/tiles/public.places/{z}/{x}/{y}.pbf',
      tilesetMetadataPath: '/kepler/tilejson/places.json',
      color: [255, 203, 5],
      mapState: {latitude: 37.77, longitude: -122.41, zoom: 11, pitch: 0, bearing: 0},
    }),
  },
  {
    id: 'divisions',
    label: 'Divisions',
    note: '~1M polygons, admin levels',
    ...mvtDataset({
      id: 'divisions',
      label: 'Overture divisions',
      tilesetDataPath: '/tiles/public.divisions/{z}/{x}/{y}.pbf',
      tilesetMetadataPath: '/kepler/tilejson/divisions.json',
      color: [120, 180, 255],
      mapState: {latitude: 20, longitude: 0, zoom: 2, pitch: 0, bearing: 0},
    }),
  },
  {
    id: 'h3-adaptive',
    label: 'H3 adaptive clustering',
    note: '32k cells, threshold 10k places',
    ...mvtDataset({
      id: 'h3-adaptive',
      label: 'H3 adaptive (t=10000)',
      tilesetDataPath: '/tiles/public.places_h3_t10000/{z}/{x}/{y}.pbf',
      tilesetMetadataPath: '/kepler/tilejson/h3-adaptive.json',
      color: [200, 100, 255],
      mapState: {latitude: 20, longitude: 0, zoom: 2, pitch: 0, bearing: 0},
    }),
  },
];
