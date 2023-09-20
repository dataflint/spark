package org.apache.spark.status.api.v1.anecdota

import org.apache.spark.status.api.v1.ApiRequestContext

import javax.ws.rs.{Path, PathParam}

@Path("/v1")
private[v1] class ApiAnecdotaSqlRootResource extends ApiRequestContext {

  @Path("applications/{appId}/devtool/sql")
  def sqlList(@PathParam("appId") appId: String): Class[AnecdotaSqlResource] = classOf[AnecdotaSqlResource]

  @Path("applications/{appId}/{attemptId}/devtool/sql")
  def sqlList(
      @PathParam("appId") appId: String,
      @PathParam("attemptId") attemptId: String): Class[AnecdotaSqlResource] = classOf[AnecdotaSqlResource]
}
