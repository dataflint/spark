import React, { FC } from 'react';
import 'reactflow/dist/style.css';
import Progress from './Progress';
import SqlFlow from './SqlFlow/SqlFlow';
import { useAppSelector } from '../Hooks';


const SqlContainer: FC = (): JSX.Element => {
  const sql = useAppSelector(state => state.spark.sql);
  return sql === undefined ?
    (
      <Progress />
    ) :
    (<div style={{ height: '100%' }}>
      <SqlFlow sparkSQL={sql.sqls[sql.sqls.length - 1]} />
    </div>);
};

export default SqlContainer;
