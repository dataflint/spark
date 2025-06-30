import GitHubIcon from "@mui/icons-material/GitHub";
import MenuBookIcon from "@mui/icons-material/MenuBook";
import StarIcon from "@mui/icons-material/Star";
import { Box, Button, Typography } from "@mui/material";
import * as React from "react";

export default function Footer() {
    const onGitHubClick = (): void => {
        window.open("https://github.com/dataflint/spark", "_blank");
    };

    const onDocsClick = (): void => {
        window.open("https://dataflint.gitbook.io/dataflint-for-spark/", "_blank");
    };

    const onDataFlintClick = (): void => {
        window.open("https://www.dataflint.io/", "_blank");
    };

    return (
        <Box
            sx={{
                display: "flex",
                justifyContent: "center",
                alignItems: "center",
                gap: 2.5,
                py: 0.5,
                px: 2,
                backgroundColor: "rgba(0, 0, 0, 0.1)",
                borderTop: 1,
                borderColor: "divider",
                minHeight: "40px"
            }}
        >
            <Button
                onClick={onGitHubClick}
                color="inherit"
                size="small"
                startIcon={<StarIcon />}
                endIcon={<GitHubIcon />}
                sx={{ fontSize: "11px", textTransform: "none" }}
            >
                Give us a star at GitHub
            </Button>

            <Button
                onClick={onDocsClick}
                color="inherit"
                size="small"
                startIcon={<MenuBookIcon />}
                sx={{ fontSize: "11px", textTransform: "none" }}
            >
                Docs
            </Button>

            <Button
                onClick={onDataFlintClick}
                color="inherit"
                size="small"
                sx={{
                    fontSize: "11px",
                    textTransform: "none",
                    display: "flex",
                    alignItems: "center",
                    gap: 0.75
                }}
            >
                <Typography variant="body2" sx={{ fontSize: "11px", fontWeight: "bold" }}>
                    Learn more about
                </Typography>
                <img
                    src="./logo.png"
                    alt="DataFlint"
                    style={{ height: "14px", width: "auto" }}
                />
            </Button>
        </Box>
    );
} 