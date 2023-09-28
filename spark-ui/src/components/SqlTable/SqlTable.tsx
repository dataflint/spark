import * as React from 'react';
import { styled } from '@mui/material/styles';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell, { tableCellClasses } from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import { EnrichedSparkSQL, SparkSQLStore } from '../../interfaces/AppStore';
import Progress from '../Progress';
import { duration } from 'moment'
import { humanFileSize, humanizeTimeDiff } from '../../utils/FormatUtils';
import { CircularProgress } from '@mui/material';
import CheckIcon from '@mui/icons-material/Check';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import { SqlStatus } from '../../interfaces/SparkSQLs';

const StyledTableCell = styled(TableCell)(({ theme }) => ({
    [`&.${tableCellClasses.head}`]: {
        backgroundColor: theme.palette.common.black,
        color: theme.palette.common.white,
    },
    [`&.${tableCellClasses.body}`]: {
        fontSize: 14,
    },
}));

const StyledTableRow = styled(TableRow)(({ theme }) => ({
    '&:nth-of-type(odd)': {
        backgroundColor: theme.palette.action.hover,
    },
    // hide last border
    '&:last-child td, &:last-child th': {
        border: 0,
    },
}));

function StatusIcon(status: string): JSX.Element {
    switch (status) {
        case SqlStatus.Running.valueOf():
            return <CircularProgress color='info' />;
        case SqlStatus.Completed.valueOf():
            return <CheckIcon color='success' />;
        case SqlStatus.Failed.valueOf():
            return <ErrorOutlineIcon color='error' />;
        default:
            return <div></div>;
    }
}


export default function SqlTable({ sqlStore, selectedSqlId, setSelectedSqlId }:
    {
        sqlStore: SparkSQLStore | undefined,
        selectedSqlId: string | undefined,
        setSelectedSqlId: React.Dispatch<React.SetStateAction<string | undefined>>
    }) {
    if (sqlStore === undefined) {
        return <Progress />;
    }

    const sqlsToShow = sqlStore.sqls.slice().filter(sql => !sql.isSqlCommand);

    return (
        <div style={{ width: "100%", display: "flex", justifyContent: "space-around" }}>
            <TableContainer component={Paper} sx={{ maxHeight: "65vh", width: "70%", }}>
                <Table stickyHeader aria-label="customized table" sx={{ margin: "auto" }}>
                    <TableHead>
                        <TableRow>
                            <StyledTableCell>id</StyledTableCell>
                            <StyledTableCell>Status</StyledTableCell>
                            <StyledTableCell>Description</StyledTableCell>
                            <StyledTableCell align="right">Duration</StyledTableCell>
                            <StyledTableCell align="right">Core/hour</StyledTableCell>
                            <StyledTableCell align="right">Activity Rate</StyledTableCell>
                            <StyledTableCell align="right">Input</StyledTableCell>
                            <StyledTableCell align="right">Output</StyledTableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {sqlsToShow.map((sql) => (
                            // sql metrics should never be null
                            sql.stageMetrics === undefined || sql.resourceMetrics === undefined ? null : 
                            <StyledTableRow sx={{ cursor: 'pointer' }} key={sql.id} selected={sql.id === selectedSqlId} onClick={(event) => setSelectedSqlId(sql.id)} >
                                <StyledTableCell component="th" scope="row">
                                    {sql.id}
                                </StyledTableCell>
                                <StyledTableCell component="th" scope="row">
                                    {StatusIcon(sql.status)}
                                </StyledTableCell>
                                <StyledTableCell component="th" scope="row">
                                    {sql.description}
                                </StyledTableCell>
                                <StyledTableCell align="right">{humanizeTimeDiff(duration(sql.duration))} ({sql.resourceMetrics.durationPercentage.toFixed(1)}%)</StyledTableCell>
                                <StyledTableCell align="right">{sql.resourceMetrics.coreHourUsage.toFixed(4)} ({sql.resourceMetrics.coreHourPercentage.toFixed(1)}%)</StyledTableCell>
                                <StyledTableCell align="right">{sql.resourceMetrics.activityRate.toFixed(2)}%</StyledTableCell>
                                <StyledTableCell align="right">{humanFileSize(sql.stageMetrics.inputBytes)}</StyledTableCell>
                                <StyledTableCell align="right">{humanFileSize(sql.stageMetrics.outputBytes)}</StyledTableCell>
                            </StyledTableRow>
                        ))}
                    </TableBody>
                </Table>
            </TableContainer>
        </div>
    );
}