import {
  getProxyBasePath,
  hrefWithoutEndSlash,
  isDataFlintSaaSUI,
  isHistoryServer,
  isProxyMode,
} from "./UrlUtils";

// In development mode, detect history server mode by checking the REACT_APP_BASE_PATH
const IS_DEV_HISTORY_MODE = process.env.NODE_ENV === "development" && 
  process.env.REACT_APP_BASE_PATH?.includes("8011");

const IS_HISTORY_SERVER_MODE = isHistoryServer() || IS_DEV_HISTORY_MODE;

let BASE_PATH = "";
let BASE_CURRENT_PAGE = hrefWithoutEndSlash();
if (process.env.NODE_ENV === "development") {
  BASE_PATH = process.env.REACT_APP_BASE_PATH ?? "";
  BASE_CURRENT_PAGE = `${BASE_PATH}/dataflint`;
} else if (isProxyMode()) {
  BASE_PATH = getProxyBasePath();
} else if (isDataFlintSaaSUI()) {
  BASE_PATH = "/dataflint-spark-ui";
}

export { BASE_CURRENT_PAGE, BASE_PATH, IS_HISTORY_SERVER_MODE };
