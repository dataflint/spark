import { ApexOptions } from "apexcharts";
import { duration } from "moment";
import React from "react";
import ReactApexChart from "react-apexcharts";
import { humanizeTimeDiff } from "../../utils/FormatUtils";

export default function DurationDistributionChart({ durationDist }: { durationDist: number[] }): JSX.Element {
    const series = [{
        name: 'Duration',
        data: durationDist
    }]

    const options: ApexOptions = {
        plotOptions: {
            bar: {
                horizontal: false
            },
        },
        chart: {
            animations: {
                enabled: false,
            },
            toolbar: {
                show: false
            },
            zoom: {
                enabled: false
            },
        },
        dataLabels: {
            enabled: false
        },
        stroke: {
            show: true,
            width: 2,
            colors: ['transparent']
        },
        xaxis: {
            categories: ['min', '0.1', '0.2', '0.3', '0.4', '0.5', '0.6', '0.7', '0.8', '0.9', 'max'],
        },
        yaxis: {
            title: {
                text: 'Duration'
            },
            labels: {
                formatter: (value: number, timestamp?: number, opts?: any) =>
                    humanizeTimeDiff(duration(value)),
            },
        },
        theme: {
            mode: "dark",
        }
    }


    return (
        <ReactApexChart
            options={options}
            series={series}
            type="bar"
            height={150}
        />
    );
};
