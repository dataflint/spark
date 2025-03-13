import React from "react";
import { Handle, Node, Position } from "reactflow";
import { StageNode } from "../StageNode/StageNode";
import styles from "./StageGroupNode.module.css";

export const StageGroupNodeName: string = "stageGroupNode";

export const isNodeAGroup = (node: Node) => node.type === StageGroupNodeName;

interface StageGroupNodeProps {
  data: {
    nodes: Node[];
    friendlyName: string;
  };
}

export default function StageGroupNode({ data }: StageGroupNodeProps) {
  const { friendlyName, nodes } = data;

  return (
    <div className={styles.groupContainer}>
      <Handle type="target" position={Position.Left} id="a" />
      <h3 className={styles.groupName}>{friendlyName}</h3>
      {nodes.map((node) => {
        return <StageNode key={node.id} data={node.data} />;
      })}
      <Handle type="source" position={Position.Right} id="b" />
    </div>
  );
}
