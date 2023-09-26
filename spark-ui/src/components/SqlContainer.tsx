import React, { FC } from 'react';
import 'reactflow/dist/style.css';
import { EnrichedSparkSQL } from '../interfaces/AppStore';
import Progress from './Progress';
import SqlFlow from './SqlFlow/SqlFlow';


const SqlContainer: FC<{sql: EnrichedSparkSQL | undefined}> = (
    {sql}): JSX.Element => {
        return sql === undefined ?
        (
          <Progress />
        ) :
        (<div style={{ height: '100%'}}>
        <SqlFlow sparkSQL={sql} />
        </div>);
};

export default SqlContainer;
