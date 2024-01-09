import Paper from "@mui/material/Paper";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import * as React from "react";
import { ConfigEntries } from "../interfaces/AppStore";

type ConfigTableProps = {
  config: ConfigEntries;
};

export default function ConfigTable({ config }: ConfigTableProps) {
  return (
    <TableContainer component={Paper}>
      <Table sx={{ minWidth: 200 }} size="small" aria-label="config table">
        <TableHead>
          <TableRow>
            <TableCell sx={{ minWidth: 150 }}>Name</TableCell>
            <TableCell align="left">Value</TableCell>
            <TableCell align="left">Documentation</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {config.map((row) => (
            <TableRow key={row.name}>
              <TableCell align="left">{row.name}</TableCell>
              <TableCell align="left">
                {row.value ?? row.default}
                {row.value === undefined || row.value === row.default
                  ? " (default)"
                  : ""}
              </TableCell>
              <TableCell component="th" scope="row">
                {row.documentation}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
}
