import React, { FC } from 'react';
import 'reactflow/dist/style.css';
import { SparkSQLStore } from '../interfaces/AppStore';
import Progress from './Progress';
import SqlFlow from './SqlFlow/SqlFlow';


const SqlContainer: FC<{currentSql: SparkSQLStore | undefined}> = (
    {currentSql}): JSX.Element => {
        return currentSql === undefined ?
        (
          <Progress />
        ) :
        (<div style={{ height: '100%'}}>
        <SqlFlow sparkSQL={currentSql.sqls[currentSql.sqls.length - 1]} />
        </div>);
};

export default SqlContainer;
