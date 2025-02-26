import CheckIcon from "@mui/icons-material/Check";
import { Box, CircularProgress, TableSortLabel } from "@mui/material";
import Paper from "@mui/material/Paper";
import { styled } from "@mui/material/styles";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell, { tableCellClasses } from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import { visuallyHidden } from "@mui/utils";
import _ from "lodash";
import { duration } from "moment";
import * as React from "react";
import { useAppDispatch, useAppSelector } from "../../Hooks";
import { EnrichedSparkSQL, SparkSQLStore } from "../../interfaces/AppStore";
import { SqlStatus } from "../../interfaces/SparkSQLs";
import { initializeVisibleColumns, setVisibleColumns } from "../../reducers/JobsColumnSlice";
import { humanFileSize, humanizeTimeDiff } from "../../utils/FormatUtils";
import { default as MultiAlertBadge } from "../AlertBadge/MultiAlertsBadge";
import ColumnPicker from "../ColumnPicker/ColumnPicker";
import ExceptionIcon from "../ExceptionIcon";
import Progress from "../Progress";
import { Data, EnhancedTableProps, Order } from './TableTypes';
import { getComparator, stableSort } from "./TableUtils";

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
  "&:nth-of-type(odd)": {
    backgroundColor: theme.palette.action.hover,
  },
  // hide last border
  "&:last-child td, &:last-child th": {
    border: 0,
  },
}));

function StatusIcon(status: string, failureReason: string): JSX.Element {
  switch (status) {
    case SqlStatus.Running.valueOf():
      return (
        <CircularProgress
          color="info"
          style={{ width: "30px", height: "30px" }}
        />
      );
    case SqlStatus.Completed.valueOf():
      return (
        <CheckIcon color="success" style={{ width: "30px", height: "30px" }} />
      );
    case SqlStatus.Failed.valueOf():
      return <ExceptionIcon failureReason={failureReason} />;
    default:
      return <div></div>;
  }
}

const createSqlTableData = (sqls: EnrichedSparkSQL[]): Data[] => {
  return sqls.flatMap((sql) => {
    return !sql.stageMetrics || !sql.resourceMetrics
      ? []
      : {
        id: sql.id,
        status: sql.status,
        description: sql.description,
        duration: sql.duration,
        durationPercentage: sql.resourceMetrics.durationPercentage,
        dcu: sql.resourceMetrics.dcu,
        dcuPercentage: sql.resourceMetrics?.dcuPercentage,
        wastedCoresRate: sql.resourceMetrics.wastedCoresRate,
        input: sql.stageMetrics.inputBytes,
        output: sql.stageMetrics.outputBytes,
        failureReason: !sql.failureReason ? "" : sql.failureReason,
        spill: sql.stageMetrics.diskBytesSpilled,
        wastedCores: sql.resourceMetrics.wastedCoresRate,
        shuffleReadBytes: sql.stageMetrics.shuffleReadBytes,
        shuffleWriteBytes: sql.stageMetrics.shuffleWriteBytes,
        totalTasks: sql.stageMetrics.totalTasks,
        executorRunTime: sql.stageMetrics.executorRunTime,
      };
  });
};

function EnhancedTableHead(props: EnhancedTableProps) {
  const { order, orderBy, onRequestSort } = props;

  const createSortHandler =
    (property: keyof Data) => (event: React.MouseEvent<unknown>) => {
      onRequestSort(event, property);
    };

  return (
    <TableHead>
      <TableRow>
        {props.headCells
          .filter((head) => {
            if (!props.visibleColumns.includes(head.id)) {
              return false;
            }
            return true
          }).map((headCell) => (
            <TableCell
              key={headCell.id}
              align={"left"}
              padding={headCell.disablePadding ? "none" : "normal"}
              sortDirection={orderBy === headCell.id ? order : false}
            >
              <TableSortLabel
                active={orderBy === headCell.id}
                direction={orderBy === headCell.id ? order : "asc"}
                onClick={createSortHandler(headCell.id)}
              >
                {headCell.label}
                {orderBy === headCell.id ? (
                  <Box component="span" sx={visuallyHidden}>
                    {order === "desc" ? "sorted descending" : "sorted ascending"}
                  </Box>
                ) : null}
              </TableSortLabel>
            </TableCell>
          ))}
      </TableRow>
    </TableHead>
  );
}

export default function SqlTable({
  sqlStore,
  selectedSqlId,
  setSelectedSqlId,
}: {
  sqlStore: SparkSQLStore | undefined;
  selectedSqlId: string | undefined;
  setSelectedSqlId: (id: string) => void;
}) {
  const dispatch = useAppDispatch();

  const [order, setOrder] = React.useState<Order>("asc");
  const [orderBy, setOrderBy] = React.useState<keyof Data>("id");
  const [sqlsTableData, setSqlsTableData] = React.useState<Data[]>([]);
  const sqlAlerts = useAppSelector(
    (state) => state.spark.alerts,
  )?.alerts.filter((alert) => alert.source.type === "sql");

  const visibleColumns = useAppSelector(
    (state) => state.jobsColumns.visibleColumns
  );

  const headers = useAppSelector((state) => state.jobsColumns.headCells);


  React.useEffect(() => {
    if (visibleColumns.length === 0) {
      dispatch(initializeVisibleColumns([]));
    }
  }, [dispatch]);

  React.useEffect(() => {
    if (!sqlStore) return;

    const sqls = createSqlTableData(sqlStore.sqls.slice());
    if (_.isEqual(sqls, sqlsTableData)) return;

    setSqlsTableData(sqls);
  }, [sqlStore]);

  const handleRequestSort = (
    event: React.MouseEvent<unknown>,
    property: keyof Data,
  ) => {
    const isAsc = orderBy === property && order === "asc";
    setOrder(isAsc ? "desc" : "asc");
    setOrderBy(property);
  };

  const visibleRows = React.useMemo(
    () => stableSort(sqlsTableData, getComparator(order, orderBy)),
    [order, orderBy, sqlsTableData],
  );

  if (sqlStore === undefined) {
    return (
      <div
        style={{
          overflow: "hidden",
          height: "100%",
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
        }}
      >
        <Progress />
      </div>
    );
  }

  const handleToggleColumn = (columns: string[]) => {
    dispatch(setVisibleColumns(columns));
  };

  return (
    <div
      style={{
        width: "100%",
        height: "100%",
        display: "flex",
        justifyContent: "space-around",
        flexDirection: "column",
        marginBottom: "15px",
        overflow: "hidden",
      }}
    >
      <Box display="flex" alignItems="center" justifyContent="center">
        <ColumnPicker
          headCells={headers}
          visibleColumns={visibleColumns}
          onToggleColumn={handleToggleColumn}
        />
      </Box>
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          justifyContent: "space-around",
          overflow: "hidden",
        }}
      >
        <TableContainer component={Paper} sx={{ width: "80%" }}>
          <Table
            size="small"
            stickyHeader
            aria-label="customized table"
            sx={{ margin: "auto" }}
          >
            <EnhancedTableHead
              visibleColumns={visibleColumns}
              headCells={headers}
              onRequestSort={handleRequestSort}
              order={order}
              orderBy={orderBy}
            />
            <TableBody>
              {visibleRows.map((sql) => (
                <StyledTableRow
                  sx={{ cursor: "pointer" }}
                  key={sql.id}
                  selected={sql.id === selectedSqlId}
                  onClick={(event) => setSelectedSqlId(sql.id)}
                >
                  {visibleColumns.includes("id") && (
                    <StyledTableCell component="th" scope="row">
                      {sql.id}
                    </StyledTableCell>)}
                  {visibleColumns.includes("status") && (
                    <StyledTableCell component="th" scope="row">
                      {StatusIcon(sql.status, sql.failureReason)}
                    </StyledTableCell>
                  )}
                  {visibleColumns.includes("description") && (
                    <StyledTableCell component="th" scope="row">
                      <Box display="flex" alignItems="center" flexWrap="wrap">
                        {sql.description}
                        {sqlAlerts !== undefined &&
                          sqlAlerts.find(
                            (alert) =>
                              alert.source.type === "sql" &&
                              alert.source.sqlId === sql.id,
                          ) !== undefined ? (
                          <MultiAlertBadge
                            alerts={sqlAlerts.filter(
                              (alert) =>
                                alert.source.type === "sql" &&
                                alert.source.sqlId === sql.id,
                            )}
                          ></MultiAlertBadge>
                        ) : null}
                      </Box>
                    </StyledTableCell>)}
                  {visibleColumns.includes("duration") && (
                    <StyledTableCell align="left">
                      {humanizeTimeDiff(duration(sql.duration))} (
                      {sql.durationPercentage.toFixed(1)}%)
                    </StyledTableCell>)}
                  {visibleColumns.includes("dcu") && (
                    <StyledTableCell align="left">
                      {sql.dcu.toFixed(4)} ({sql.dcuPercentage.toFixed(1)}
                      %)
                    </StyledTableCell>)}
                  {visibleColumns.includes("input") && (
                    <StyledTableCell align="left">
                      {humanFileSize(sql.input)}
                    </StyledTableCell>)}
                  {visibleColumns.includes("output") && (
                    <StyledTableCell align="left">
                      {humanFileSize(sql.output)}
                    </StyledTableCell>)}
                  {visibleColumns.includes("spill") && (
                    <StyledTableCell align="left">
                      {humanFileSize(sql.spill)}
                    </StyledTableCell>)}
                  {visibleColumns.includes("idleCores") && (
                    <StyledTableCell align="left">
                      {sql.wastedCores.toFixed(2)}%
                    </StyledTableCell>)}
                  {visibleColumns.includes("shuffleReadBytes") && (
                    <StyledTableCell align="left">
                      {humanFileSize(sql.shuffleReadBytes)}
                    </StyledTableCell>)}
                  {visibleColumns.includes("shuffleWriteBytes") && (
                    <StyledTableCell align="left">
                      {humanFileSize(sql.shuffleWriteBytes)}
                    </StyledTableCell>)}
                  {visibleColumns.includes("totalTasks") && (
                    <StyledTableCell align="left">
                      {sql.totalTasks}
                    </StyledTableCell>)}
                  {visibleColumns.includes("executorRunTime") && (
                    <StyledTableCell align="left">
                      {humanizeTimeDiff(duration(sql.executorRunTime))}
                    </StyledTableCell>)
                  }
                </StyledTableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </div>
    </div>
  );
}
