import { ApexOptions } from "apexcharts";
import { duration } from "moment";
import React from "react";
import ReactApexChart from "react-apexcharts";
import { ExecutorTimelinePoints } from "../../interfaces/AppStore";
import { truncateString } from "../../reducers/PlanParsers/PlanParserUtils";
import { humanizeTimeDiff } from "../../utils/FormatUtils";
import { DISTINCT_COLORS } from "./ColorsOutput";

export interface StaticResource {
  type: "static";
  instances: number;
}

export interface DynamicResource {
  type: "dynamic";
  min: number;
  max: number | undefined;
}

export interface Query {
  id: string;
  name: string;
  start: number;
  end: number;
}

interface SteplineGraphProps {
  data: ExecutorTimelinePoints;
  resources: StaticResource | DynamicResource | undefined;
  queries: Query[];
}

const ResourceGraph: React.FC<SteplineGraphProps> = ({
  data,
  resources,
  queries,
}) => {
  // Transforming data for the graph
  const series = [
    {
      name: "Executors",
      data: data.map((point) => ({ x: point.timeMs, y: point.value })),
    },
  ];

  const yannotations: YAxisAnnotations[] = [];
  if (resources?.type === "static") {
    yannotations.push({
      y: resources.instances,
      borderColor: "#00E396",
      label: {
        borderColor: "#00E396",
        style: {
          color: "#fff",
          background: "#00E396",
        },
        text: "Target",
      },
    });
  }
  if (resources?.type === "dynamic") {
    yannotations.push({
      y: resources?.min,
      borderColor: "#00E396",
      label: {
        borderColor: "#00E396",
        style: {
          color: "#fff",
          background: "#00E396",
        },
        text: "Min",
      },
    });
    if (resources.max !== undefined) {
      yannotations.push({
        y: resources?.max,
        strokeDashArray: 0,
        borderColor: "#775DD0",
        label: {
          borderColor: "#775DD0",
          style: {
            color: "#fff",
            background: "#775DD0",
          },
          text: "Max",
        },
      });
    }
  }

  var palette = DISTINCT_COLORS;
  const xannotations: XAxisAnnotations[] = queries.map((query, i) => {
    // first color is ugly so we skip it (i + 1)
    const color =
      i < palette.length
        ? palette[i + 1]
        : palette[i % (palette.length - 1)];
    return {
      x: query.start,
      x2: query.end,
      borderColor: "#000",
      fillColor: color,
      opacity: 0.2,
      label: {
        borderColor: "#333",
        style: {
          fontSize: "10px",
          color: "#333",
          background: color,
        },
        text: truncateString(`${query.id}: ${query.name}`, 30),
      },
    };
  });

  const options: ApexOptions = {
    chart: {
      type: "line",
      height: 350,
    },
    annotations: {
      yaxis: yannotations,
      xaxis: xannotations,
    },
    dataLabels: {
      enabled: false,
    },
    stroke: {
      curve: "stepline",
    },
    xaxis: {
      type: "numeric",
      labels: {
        formatter: (value: string, timestamp?: number, opts?: any) =>
          humanizeTimeDiff(duration(value), false),
      },
    },
    yaxis: {
      decimalsInFloat: 0, // Add this line to remove decimal points
    },
    theme: {
      mode: "dark",
    },
    title: {
      text: "Executors Timeline",
      align: "left",
    },
  };

  return (
    <ReactApexChart
      options={options}
      series={series}
      type="line"
      height={350}
    />
  );
};

export default ResourceGraph;
