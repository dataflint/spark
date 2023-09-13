import { SqlResponse } from "../interfaces/SqlInterfaces";

class API {
    static BASE_PATH = process.env.NODE_ENV === 'development' ? "http://localhost:10000": "";
    static API_PATH = `${this.BASE_PATH}/api/v1`;
    static APPLICATION_PATH = `${this.API_PATH}/applications`;
    static SQL = "sql"


    static async getSqlData(appId: string): Promise<SqlResponse | undefined> {
        try {
            var path = `${API.APPLICATION_PATH}/${appId}/${API.SQL}`;
            const result = await fetch(path);
            const resultJson = await result.json();
            console.log(resultJson);
            return resultJson;
        }
        catch (e) {
            console.error(e);
        }
    }
}

export default API;