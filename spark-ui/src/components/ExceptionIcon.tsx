import ErrorOutlineIcon from "@mui/icons-material/ErrorOutline";
import {
    Fade, Snackbar
} from "@mui/material";
import Tooltip, { TooltipProps, tooltipClasses } from "@mui/material/Tooltip";
import { styled } from "@mui/material/styles";
import * as React from "react";


const CustomWidthTooltip = styled(({ className, ...props }: TooltipProps) => (
    <Tooltip {...props} classes={{ popper: className }} />
))({
    [`& .${tooltipClasses.tooltip}`]: {
        maxWidth: 500,
        maxHeight: 300,
        overflow: "auto",
        whiteSpace: "pre",
    },
});

const onTooltipClick = (
    event: React.MouseEvent<unknown>,
    failureReason: string,
    setOpenSnackbar: React.Dispatch<React.SetStateAction<boolean>>,
) => {
    event.stopPropagation();
    setOpenSnackbar(true);
    navigator.clipboard.writeText(failureReason);
};

const formatFailureReason = (failureReason: string) => {
    const regex = /(Caused by:.*?)(?=\n)/s;
    const match = regex.exec(failureReason);

    if (match) {
        const causedByText = match[1].trim();
        return `${causedByText}\nFull stacktrace:\n${failureReason}`;
    }

    return failureReason;
};

const ExceptionIcon: React.FC<{ failureReason: string }> = ({ failureReason }): JSX.Element => {
    const [openSnackbar, setOpenSnackbar] = React.useState<boolean>(false);

    const handleClose = (
        event: React.SyntheticEvent | Event,
        reason?: string,
    ) => {
        if (reason === "clickaway") {
            return;
        }

        setOpenSnackbar(false);
    };

    const formatedFailureReason = formatFailureReason(failureReason);
    return (
        <div
            onClick={(event) =>
                onTooltipClick(event, failureReason, setOpenSnackbar)
            }
        >
            <CustomWidthTooltip
                arrow
                placement="top"
                style={{ overflow: "auto" }}
                title={formatedFailureReason}
                TransitionComponent={Fade}
                TransitionProps={{ timeout: 300 }}
            >
                <ErrorOutlineIcon
                    color="error"
                    style={{ width: "30px", height: "30px" }}
                />
            </CustomWidthTooltip>
            <Snackbar
                onClose={handleClose}
                open={openSnackbar}
                autoHideDuration={2000}
                message="Copied to clip board"
            />

        </div>
    );
}

export default ExceptionIcon;
