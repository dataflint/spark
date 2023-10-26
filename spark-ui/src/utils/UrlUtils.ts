
export const isHistoryServer = (): boolean => window.location.href.includes('history');

export const isProxyMode = (): boolean => !(window.location.pathname === "/dataflint" || window.location.pathname === "/dataflint/");

export function hrefWithoutEndSlash(): string {
    const href = window.location.href;

    // We are using a HashRouter so we split by #
    const baseUrl = href.split('/#/')[0];
    if (baseUrl.endsWith("/")) {
        return baseUrl.substring(0, href.length - 1);
    }
    return baseUrl;
}

export const getProxyBasePath = (): string => hrefWithoutEndSlash().substring(0, hrefWithoutEndSlash().lastIndexOf("/dataflint"));

export function getHistoryServerCurrentAppId(): string {
    const urlSegments = hrefWithoutEndSlash().split('/');
    try {
        const historyIndex = urlSegments.findIndex(segment => segment === 'history');
        const appId = urlSegments[historyIndex + 1];
        return appId;
    }
    catch {
        throw new Error("Invalid history server app id");
    }
}

export const getBaseAppUrl = (appPath: string): string => {
    return appPath.split('/dataflint')[0];
}
