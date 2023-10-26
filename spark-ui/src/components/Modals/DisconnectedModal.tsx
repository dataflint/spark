import { Box, Fade, Modal, Typography } from "@mui/material";
import React, { useContext } from "react";
import { FC } from "react";
import Backdrop from '@mui/material/Backdrop';
import { useAppSelector } from "../../Hooks";


const style = {
    position: 'absolute' as 'absolute',
    top: '50%',
    left: '50%',
    transform: 'translate(-50%, -50%)',
    width: 400,
    bgcolor: '#383838',
    outline: 'none',
    borderRadius: '4px',
    boxShadow: 24,
    p: 4,
};

const DisconnectedModal: FC = (): JSX.Element => {
    const { isConnected, isInitialized } = useAppSelector(state => state.spark);

    const open = !isConnected && isInitialized

    return (
        <Modal
            aria-labelledby="transition-modal-title"
            aria-describedby="transition-modal-description"
            open={open}
            slots={{ backdrop: Backdrop }}
            slotProps={{
                backdrop: {
                    timeout: 500,
                },
            }}
        >
            <Fade in={open}>
                <Box sx={style}>
                    <Typography id="transition-modal-title" variant="h6" component="h2">
                        Server disconnected
                    </Typography>
                    <Typography id="transition-modal-description" sx={{ mt: 2 }}>
                        Trying to reconnect...
                    </Typography>
                </Box>
            </Fade>
        </Modal>
    );

}

export default DisconnectedModal;