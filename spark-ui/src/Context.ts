import { createContext } from 'react';

export default interface AppState {
    isConnected: boolean
    isInitialized: boolean
}

export const AppStateContext = createContext<AppState>({ isConnected: false, isInitialized: false });
