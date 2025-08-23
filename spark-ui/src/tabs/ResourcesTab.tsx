import {
  Alert,
  Box,
  Divider,
  FormControlLabel,
  FormGroup,
  Switch,
} from "@mui/material";
import React, { FC } from "react";
import ConfigTable from "../components/ConfigTable";
import ResourceBar from "../components/ResourceBar";
import ResourceGraph, {
  DynamicResource,
  Query,
  StaticResource,
} from "../components/ResourceGraph/ResourceGraph";
import { useAppSelector } from "../Hooks";

export const ResourcesTab: FC<{}> = (): JSX.Element => {
  const resourceControlType =
    useAppSelector((state) => state.spark.config?.resourceControlType) ?? "";

  if (resourceControlType === "local") {
    return (
      <div
        style={{
          height: "100%",
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
        }}
      >
        <Alert severity="success">Local mode, no resource managment 😎</Alert>
      </div>
    );
  }

  const executorTimeline = useAppSelector(
    (state) => state.spark.executorTimeline,
  );
  const executors = useAppSelector(
    (state) => state.spark.executors,
  );

  const maxCores = Math.max(...executors?.filter((executor) => !executor.isDriver).map((executor) => executor.totalCores) ?? [1]).toString();

  const configs = useAppSelector((state) => state.spark.config?.configs);
  const sqls = useAppSelector((state) => state.spark.sql?.sqls) ?? [];
  const startTime =
    useAppSelector((state) => state.spark.runMetadata?.startTime) ?? 0;

  const [showQueries, setShowQueries] = React.useState(false);

  const generalConfigs =
    configs?.filter((entry) => entry.category === "resources").map(
      (entry) => {
        if (entry.key === "spark.executor.cores") {
          return { ...entry, value: entry.value ?? maxCores.toString() };
        }
        return entry;
      }
    ) ?? [];
  const allocationConfigs =
    configs?.filter((entry) => {
      if (resourceControlType === "static") {
        return entry.category === "static-allocation";
      }
      if (resourceControlType === "dynamic") {
        return (
          entry.name !== "enabled" &&
          (entry.category === "dynamic-allocation" ||
            entry.category === "dynamic-allocation-advanced")
        );
      }
      if (resourceControlType === "databricks") {
        return entry.category === "databricks-static-allocation" || entry.category === "databricks-autoscale";
      }
      return false;
    }) ?? [];

  let resources: StaticResource | DynamicResource | undefined = undefined;
  if (resourceControlType === "static") {
    resources = {
      type: "static",
      instances: parseInt(
        allocationConfigs.find(
          (entry) => entry.key === "spark.executor.instances",
        )?.value ?? "0",
      ),
    };
  }
  if (resourceControlType === "dynamic") {
    const minEntry = allocationConfigs.find(
      (entry) => entry.key === "spark.dynamicAllocation.minExecutors",
    );
    const maxEntry = allocationConfigs.find(
      (entry) => entry.key === "spark.dynamicAllocation.maxExecutors",
    );

    resources = {
      type: "dynamic",
      min: parseInt(minEntry?.value ?? minEntry?.default ?? "0"),
      max: maxEntry?.value === undefined ? undefined : parseInt(maxEntry.value),
    };
  }

  if (resourceControlType === "databricks") {
    const minEntry = allocationConfigs.find(
      (entry) => entry.key === "spark.databricks.clusterUsageTags.minExecutors",
    );
    const maxEntry = allocationConfigs.find(
      (entry) => entry.key === "spark.databricks.clusterUsageTags.maxExecutors",
    );
    const instancesEntry = configs?.find(
      (entry) => entry.key === "spark.databricks.clusterUsageTags.clusterWorkers",
    );

    if (minEntry !== undefined && maxEntry !== undefined) {
      resources = {
        type: "dynamic",
        min: parseInt(minEntry?.value ?? minEntry?.default ?? "0"),
        max: maxEntry?.value === undefined ? undefined : parseInt(maxEntry.value),
      };
    } else if (instancesEntry !== undefined) {
      resources = {
        type: "static",
        instances: parseInt(instancesEntry.value ?? instancesEntry.default ?? "0"),
      };
    }
  }


  const queries: Query[] = showQueries
    ? sqls.map((sql) => {
      return {
        id: sql.id,
        name: sql.description,
        start: sql.submissionTimeEpoc - startTime,
        end: sql.submissionTimeEpoc + sql.duration - startTime,
      };
    })
    : [];

  return (
    <div
      style={{
        height: "100%",
        display: "flex",
        justifyContent: "center",
        margin: 10,
        overflow: "auto",
      }}
    >
      <Box width="100%">
        <ResourceBar />
        <Box width="100%">
          <Box margin="10px">
            <ResourceGraph
              data={executorTimeline ?? []}
              resources={resources}
              queries={queries}
            ></ResourceGraph>
          </Box>
          <Box display="flex" justifyContent="center">
            <FormGroup>
              <FormControlLabel
                control={
                  <Switch
                    checked={showQueries}
                    onChange={(evnt) => setShowQueries(evnt.target.checked)}
                    inputProps={{ "aria-label": "controlled" }}
                  />
                }
                label="Show Queries"
              />
            </FormGroup>
          </Box>
          <Box
            margin="10px"
            display="flex"
            flexDirection="row"
            alignItems="flex-start"
          >
            {generalConfigs.length !== 0 ? (
              <ConfigTable config={generalConfigs} />
            ) : undefined}
            <Divider sx={{ margin: 1 }}></Divider>
            {allocationConfigs.length !== 0 ? (
              <ConfigTable config={allocationConfigs} />
            ) : undefined}
          </Box>
        </Box>
      </Box>
    </div>
  );
};
