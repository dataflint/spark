export const isHistoryServer = (): boolean =>
  window.location.href.includes("history");

export const isProxyMode = (): boolean =>
  !(
    window.location.pathname === "/dataflint" ||
    window.location.pathname === "/dataflint/"
  );

export function hrefWithoutEndSlash(): string {
  const href = window.location.href;
  let fixedUrl = href.split("/#/")[0];

  // We are using a HashRouter so we split by #
  if (fixedUrl.endsWith("index.html")) {
    fixedUrl = fixedUrl.substring(0, fixedUrl.length - "index.html".length)
  }
  if (fixedUrl.endsWith("index2.html")) {
    fixedUrl = fixedUrl.substring(0, fixedUrl.length - "index2.html".length)
  }
  if (fixedUrl.includes("?o=")) {
    fixedUrl = fixedUrl.split("dataflint")[0] + "dataflint"
  }
  if (fixedUrl.endsWith("/")) {
    fixedUrl = fixedUrl.substring(0, fixedUrl.length - 1);
  }
  return fixedUrl;
}

export const getProxyBasePath = (): string =>
  hrefWithoutEndSlash().substring(
    0,
    hrefWithoutEndSlash().lastIndexOf("/dataflint"),
  );

export function getHistoryServerCurrentAppId(): string {
  const urlSegments = hrefWithoutEndSlash().split("/");
  try {
    const historyIndex = urlSegments.findIndex(
      (segment) => segment === "history",
    );
    const appId = urlSegments[historyIndex + 1];
    return appId;
  } catch {
    throw new Error("Invalid history server app id");
  }
}

export const getBaseAppUrl = (appPath: string): string => {
  return appPath.split("/dataflint")[0];
};
