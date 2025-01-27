import {
  Checkbox,
  FormControl,
  InputLabel,
  ListItemText,
  MenuItem,
  OutlinedInput,
  Select,
} from "@mui/material";
import React from "react";

const ITEM_HEIGHT = 48;
const ITEM_PADDING_TOP = 8;
const MenuProps = {
  PaperProps: {
    style: {
      maxHeight: ITEM_HEIGHT * 4.5 + ITEM_PADDING_TOP,
      width: 300,
    },
  },
};

interface ColumnPickerProps {
  headCells: { id: string; label: string }[];
  visibleColumns: string[];
  onToggleColumn: (columnId: string[]) => void;
}

const ColumnPicker: React.FC<ColumnPickerProps> = ({
  headCells,
  visibleColumns,
  onToggleColumn,
}) => {
  const handleChange = (event: any) => {
    const {
      target: { value },
    } = event;
    // Update visible columns
    onToggleColumn(typeof value === "string" ? value.split(",") : value);
  };

  return (
    <FormControl sx={{ m: 1, width: 300 }}>
      <InputLabel id="column-picker-label">Columns</InputLabel>
      <Select
        labelId="column-picker-label"
        id="column-picker"
        multiple
        value={visibleColumns}
        onChange={handleChange}
        input={<OutlinedInput label="Columns" />}
        renderValue={(selected) =>
          headCells
            .filter((headCell) => selected.includes(headCell.id))
            .map((headCell) => headCell.label)
            .join(", ")
        }
        MenuProps={MenuProps}
      >
        {headCells.map((headCell) => (
          <MenuItem key={headCell.id} value={headCell.id}>
            <Checkbox checked={visibleColumns.includes(headCell.id)} />
            <ListItemText primary={headCell.label} />
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  );
};

export default ColumnPicker;