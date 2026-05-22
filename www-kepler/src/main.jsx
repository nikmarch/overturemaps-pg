import React from 'react';
import {createRoot} from 'react-dom/client';
import {Provider} from 'react-redux';
import {configureStore, combineReducers} from '@reduxjs/toolkit';
import {taskMiddleware} from 'react-palm/tasks';
import keplerGlReducer from '@kepler.gl/reducers';

import App from './App.jsx';

const rootReducer = combineReducers({
  keplerGl: keplerGlReducer,
});

const store = configureStore({
  reducer: rootReducer,
  middleware: (getDefault) =>
    // kepler.gl payloads contain non-serializable values (Maplibre map
    // instances, layer classes). Disabling the check is the documented path.
    // taskMiddleware is required: kepler dispatches react-palm Tasks for all
    // async side-effects (remote tile metadata fetches, dataset creation,
    // etc.). Without it, withTask() drops the tasks and nothing ever fetches.
    getDefault({serializableCheck: false, immutableCheck: false}).concat(taskMiddleware),
});

if (typeof window !== 'undefined') window.__store = store;

createRoot(document.getElementById('root')).render(
  <Provider store={store}>
    <App />
  </Provider>
);
