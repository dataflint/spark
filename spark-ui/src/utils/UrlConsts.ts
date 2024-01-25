import {
  getProxyBasePath,
  hrefWithoutEndSlash,
  isDataFlintSaaSUI,
  isHistoryServer,
  isProxyMode,
} from "./UrlUtils";

const IS_HISTORY_SERVER_MODE = isHistoryServer();

let BASE_PATH = "";
let BASE_CURRENT_PAGE = hrefWithoutEndSlash();
if (process.env.NODE_ENV === "development") {
  BASE_PATH = process.env.REACT_APP_BASE_PATH ?? "";
  BASE_CURRENT_PAGE = `${BASE_PATH}/dataflint`;
} else if (!IS_HISTORY_SERVER_MODE && isProxyMode()) {
  BASE_PATH = getProxyBasePath();
} else if (isDataFlintSaaSUI()) {
  BASE_PATH = "/dataflint-spark-ui";
}

export { BASE_CURRENT_PAGE, BASE_PATH, IS_HISTORY_SERVER_MODE };
