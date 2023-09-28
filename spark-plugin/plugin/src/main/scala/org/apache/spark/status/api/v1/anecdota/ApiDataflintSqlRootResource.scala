package org.apache.spark.status.api.v1.dataflint

import org.apache.spark.status.api.v1.ApiRequestContext

import javax.ws.rs.{Path, PathParam}

@Path("/v1")
private[v1] class ApiDataflintSqlRootResource extends ApiRequestContext {

  @Path("applications/{appId}/dataflint/sql")
  def sqlList(@PathParam("appId") appId: String): Class[DataflintSqlResource] = classOf[DataflintSqlResource]

  @Path("applications/{appId}/{attemptId}/dataflint/sql")
  def sqlList(
      @PathParam("appId") appId: String,
      @PathParam("attemptId") attemptId: String): Class[DataflintSqlResource] = classOf[DataflintSqlResource]
}
