package org.acme.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import io.smallrye.mutiny.Uni;

@RegisterRestClient(configKey = "igp-api")
@ApplicationScoped
public interface IgpClient {

    @GET
    @Path("/api/ultimo-sismo")  // ✅ NUEVA RUTA
    @Produces(MediaType.APPLICATION_JSON)
    Uni<String> getUltimoSismo();
}
