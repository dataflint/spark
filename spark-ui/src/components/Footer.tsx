import GitHubIcon from "@mui/icons-material/GitHub";
import MenuBookIcon from "@mui/icons-material/MenuBook";
import OndemandVideoIcon from "@mui/icons-material/OndemandVideo";
import StarIcon from "@mui/icons-material/Star";
import NewReleasesIcon from "@mui/icons-material/NewReleases";
import { Box, Button, Chip, Typography } from "@mui/material";
import * as React from "react";

function isNewerVersion(latest: string, current: string): boolean {
    const latestParts = latest.split(".").map(Number);
    const currentParts = current.split(".").map(Number);
    for (let i = 0; i < Math.max(latestParts.length, currentParts.length); i++) {
        const l = latestParts[i] || 0;
        const c = currentParts[i] || 0;
        if (l > c) return true;
        if (l < c) return false;
    }
    return false;
}

function useLatestVersion(): string | null {
    const [latestVersion, setLatestVersion] = React.useState<string | null>(null);

    React.useEffect(() => {
        const url = "https://central.sonatype.com/api/internal/browse/component/versions?sortField=normalizedVersion&sortDirection=desc&page=0&size=1&filter=namespace:io.dataflint,name:spark_2.12";
        fetch(url)
            .then(res => res.json())
            .then(data => {
                const version = data?.components?.[0]?.version;
                if (version) setLatestVersion(version);
            })
            .catch(() => {});
    }, []);

    return latestVersion;
}

export default function Footer() {
    const currentVersion = process.env.REACT_APP_VERSION;
    const latestVersion = useLatestVersion();
    const hasUpdate = latestVersion && currentVersion && isNewerVersion(latestVersion, currentVersion);

    const onGitHubClick = (): void => {
        window.open("https://github.com/dataflint/spark", "_blank");
    };

    const onDocsClick = (): void => {
        window.open("https://dataflint.gitbook.io/dataflint-for-spark/", "_blank");
    };

    const onYouTubeClick = (): void => {
        window.open("https://www.youtube.com/watch?v=4d_jBCmodKQ", "_blank");
    };

    const onDataFlintClick = (): void => {
        window.open("https://www.dataflint.io/", "_blank");
    };

    const onUpdateClick = (): void => {
        window.open("https://github.com/dataflint/spark/releases", "_blank");
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
            {hasUpdate && (
                <Chip
                    icon={<NewReleasesIcon />}
                    label={`New version ${latestVersion} available!`}
                    size="small"
                    color="primary"
                    variant="outlined"
                    onClick={onUpdateClick}
                    sx={{ fontSize: "11px", cursor: "pointer" }}
                />
            )}

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
                onClick={onYouTubeClick}
                color="inherit"
                size="small"
                startIcon={<OndemandVideoIcon />}
                sx={{ fontSize: "11px", textTransform: "none" }}
            >
                YouTube Tutorial
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