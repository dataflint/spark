
export type Order = "asc" | "desc";

export interface EnhancedTableProps {
  onRequestSort: (
    event: React.MouseEvent<unknown>,
    property: keyof Data,
  ) => void;
  order: Order;
  orderBy: string;
}

export interface Data {
  id: string;
  status: string;
  description: string;
  duration: number;
  durationPercentage: number;
  dfu: number;
  dfuPercentage: number;
  activityRate: number;
  input: number;
  output: number;
  failureReason: string;
}

export interface HeadCell {
  disablePadding: boolean;
  id: keyof Data;
  label: string;
  numeric: boolean;
}
