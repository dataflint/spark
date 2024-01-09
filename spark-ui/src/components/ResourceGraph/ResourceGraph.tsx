import { ApexOptions } from "apexcharts";
import { duration } from "moment";
import React from "react";
import ReactApexChart from "react-apexcharts";
import { ExecutorTimelinePoints } from "../../interfaces/AppStore";
import { humanizeTimeDiff } from "../../utils/FormatUtils";

export interface StaticResource {
    type: "static"
    instances: number
}

export interface DynamicResource {
    type: "dynamic"
    min: number
    max: number | undefined
}

interface SteplineGraphProps {
    data: ExecutorTimelinePoints;
    resources: StaticResource | DynamicResource | undefined
}

const ResourceGraph: React.FC<SteplineGraphProps> = ({ data, resources }) => {
    // Transforming data for the graph
    const series = [
        {
            name: "Executors",
            data: data.map((point) => ({ x: point.timeMs, y: point.value })),
        },
    ];

    const annotations: YAxisAnnotations[] = [];
    if (resources?.type === "static") {
        annotations.push({
            y: resources.instances,
            borderColor: '#00E396',
            label: {
                borderColor: '#00E396',
                style: {
                    color: '#fff',
                    background: '#00E396',
                },
                text: 'Target',
            }
        })
    }
    if (resources?.type === "dynamic") {
        annotations.push({
            y: resources?.min,
            borderColor: '#00E396',
            label: {
                borderColor: '#00E396',
                style: {
                    color: '#fff',
                    background: '#00E396',
                },
                text: 'Min',
            }
        });
        if (resources.max !== undefined) {
            annotations.push({
                y: resources?.max,
                strokeDashArray: 0,
                borderColor: '#775DD0',
                label: {
                    borderColor: '#775DD0',
                    style: {
                        color: '#fff',
                        background: '#775DD0',
                    },
                    text: 'Max',
                }
            });
        }
    }



    const options: ApexOptions = {
        chart: {
            type: "line",
            height: 350
        },
        annotations: {
            yaxis: annotations
        },
        dataLabels: {
            enabled: false
        },
        stroke: {
            curve: "stepline",
        },
        xaxis: {
            type: "numeric",
            labels: {
                formatter: (value: string, timestamp?: number, opts?: any) => humanizeTimeDiff(duration(value))
            },
        },
        yaxis: {
            decimalsInFloat: 0, // Add this line to remove decimal points
        },
        theme: {
            mode: 'dark',
        },
        title: {
            text: 'Executors Timeline',
            align: 'left',
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
