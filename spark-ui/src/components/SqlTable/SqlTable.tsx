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

function createData(
    name: string,
    calories: number,
    fat: number,
    carbs: number,
    protein: number,
) {
    return { name, calories, fat, carbs, protein };
}

const rows = [
    createData('Frozen yoghurt', 159, 6.0, 24, 4.0),
    createData('Ice cream sandwich', 237, 9.0, 37, 4.3),
    createData('Eclair', 262, 16.0, 24, 6.0),
    createData('Cupcake', 305, 3.7, 67, 4.3),
    createData('Gingerbread', 356, 16.0, 49, 3.9),
];

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
            <TableContainer component={Paper} sx={{ maxHeight: "75vh", width: "70%", }}>
                <Table stickyHeader aria-label="customized table" sx={{ margin: "auto" }}>
                    <TableHead>
                        <TableRow>
                            <StyledTableCell>id</StyledTableCell>
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
                            sql.metrics === undefined ? null : 
                            <StyledTableRow sx={{ cursor: 'pointer' }} key={sql.id} selected={sql.id === selectedSqlId} onClick={(event) => setSelectedSqlId(sql.id)} >
                                <StyledTableCell component="th" scope="row">
                                    {sql.id}
                                </StyledTableCell>
                                <StyledTableCell component="th" scope="row">
                                    {sql.description}
                                </StyledTableCell>
                                <StyledTableCell align="right">{humanizeTimeDiff(duration(sql.duration))}</StyledTableCell>
                                <StyledTableCell align="right">{1234}</StyledTableCell>
                                <StyledTableCell align="right">{1234}</StyledTableCell>
                                <StyledTableCell align="right">{humanFileSize(sql.metrics.inputBytes)}</StyledTableCell>
                                <StyledTableCell align="right">{humanFileSize(sql.metrics.outputBytes)}</StyledTableCell>
                            </StyledTableRow>
                        ))}
                    </TableBody>
                </Table>
            </TableContainer>
        </div>
    );
}