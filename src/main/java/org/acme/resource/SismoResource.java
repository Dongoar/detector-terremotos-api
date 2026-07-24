package org.acme.resource;

import io.quarkus.panache.common.Sort;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.acme.model.Sismo;
import org.acme.service.SismoEventBus;
import org.acme.service.IgpClient;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.resteasy.reactive.RestStreamElementType;

import java.time.Duration;
import java.util.List;
import java.util.logging.Logger;

@Path("/api/sismos")
@ApplicationScoped
public class SismoResource {

    private static final Logger LOGGER = Logger.getLogger(SismoResource.class.getName());

    @Inject
    SismoEventBus eventBus;

    @Inject
    @RestClient
    IgpClient igpClient;

@GET
@Produces(MediaType.APPLICATION_JSON)
public Uni<List<Sismo>> obtenerHistorial() {
    LOGGER.info("📊 Obteniendo historial de sismos");
    // ✅ Limitar a los 50 más recientes
    return Sismo.find("ORDER BY fechaHora DESC")
            .page(0, 100)
            .list();
}

    @GET
    @Path("/en-vivo")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    @RestStreamElementType(MediaType.APPLICATION_JSON)
    public Multi<Sismo> transmitirSismos() {
        LOGGER.info("📡 Cliente conectado al stream SSE");

        Multi<Sismo> heartbeat = Multi.createFrom().ticks().every(Duration.ofSeconds(5))
                .onItem().transform(tick -> null);

        return Multi.createBy().merging()
                .streams(eventBus.getProcessor(), heartbeat)
                .filter(item -> item != null)
                .onFailure().recoverWithItem(() -> {
                    LOGGER.warning("⚠️ Error en SSE, recuperando...");
                    return null;
                })
                .filter(item -> item != null);
    }
@GET
@Path("/test-igp")
@Produces(MediaType.APPLICATION_JSON)
public Uni<String> testIgp() {
    LOGGER.info("🧪 Probando integración con IGP...");
    return igpClient.getUltimoSismo()
            .onItem().invoke(response -> LOGGER.info("✅ IGP respondió: " + response))
            .onFailure().invoke(err -> LOGGER.severe("❌ Error IGP: " + err.getMessage()));
}

    @DELETE
    @Path("/{id}")
    @WithTransaction
    public Uni<Boolean> eliminarSismo(@PathParam("id") Long id) {
        return Sismo.deleteById(id);
    }

    @DELETE
    @Path("/limpiar")
    @WithTransaction
    public Uni<Long> limpiarBaseDeDatos() {
        return Sismo.deleteAll();
    }

    @DELETE
    @Path("/limpiar/usuario/{usuarioId}")
    @WithTransaction
    public Uni<Long> limpiarSismosPorUsuario(@PathParam("usuarioId") String usuarioId) {
        LOGGER.info("🗑️ Eliminando sismos del usuario: " + usuarioId);
        return Sismo.delete("usuarioId = ?1", usuarioId);
    }

    @GET
    @Path("/usuario/{usuarioId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<List<Sismo>> obtenerSismosPorUsuario(@PathParam("usuarioId") String usuarioId) {
        LOGGER.info("📊 Obteniendo sismos del usuario: " + usuarioId);
        return Sismo.find("usuarioId = ?1", usuarioId).list();
    }
}
