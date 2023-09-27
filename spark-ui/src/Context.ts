import { createContext } from 'react';
import { AppStore } from './interfaces/AppStore';
import { initialState } from './reducers/SparkReducer';


export const AppStateContext = createContext<AppStore>(initialState);
