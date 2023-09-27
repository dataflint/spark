import * as React from 'react';
import StatusBar from '../components/StatusBar';
import SqlContainer from '../components/SqlContainer';

export default function StatusTab() {
  return (
    <div style={{ display: "flex", height: "100%", flexDirection: "column" }}>
      <StatusBar />
      <SqlContainer />
    </div>
  );
}