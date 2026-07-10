package org.acme.service;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import io.smallrye.mutiny.Uni;

@RegisterRestClient(configKey = "usgs-earthquake-api")
public interface EarthquakeClient {

    @GET
    @Path("/fdsnws/event/1/query")
    @Produces(MediaType.APPLICATION_JSON)
    Uni<String> getRecentEarthquakes(
            @QueryParam("format") String format,
            @QueryParam("minmagnitude") double minMagnitude
    );
}