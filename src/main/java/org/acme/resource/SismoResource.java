package org.acme.resource;

import io.quarkus.panache.common.Sort;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.acme.model.Sismo;
import org.acme.service.SismoEventBus;
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

    // ✅ GET con CORS manual (reactivo)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Response> obtenerHistorial() {
        LOGGER.info("📊 Obteniendo historial de sismos");

        return Sismo.listAll(Sort.by("fechaHora").descending())
                .onItem().transform(sismos -> {
                    return Response.ok(sismos)
                            .header("Access-Control-Allow-Origin", "*")
                            .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                            .header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Origin, X-Requested-With")
                            .header("Access-Control-Expose-Headers", "Content-Type, Cache-Control")
                            .build();
                });
    }

    // ✅ GET /en-vivo con HEARTBEAT y CORS manual
    @GET
    @Path("/en-vivo")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    @RestStreamElementType(MediaType.APPLICATION_JSON)
    public Response transmitirSismos() {
        LOGGER.info("📡 Cliente conectado al stream SSE");

        // ✅ HEARTBEAT cada 5 segundos (más frecuente para Cloud Run)
        Multi<Sismo> heartbeat = Multi.createFrom().ticks().every(Duration.ofSeconds(5))
                .onItem().transform(tick -> null);

        // ✅ Combinar eventos con heartbeat
        Multi<Sismo> stream = Multi.createBy().merging()
                .streams(eventBus.getProcessor(), heartbeat)
                .filter(item -> item != null)
                .onFailure().recoverWithItem(() -> {
                    LOGGER.warning("⚠️ Error en SSE, recuperando...");
                    return null;
                })
                .filter(item -> item != null);

        // ✅ CORS manual + headers para SSE
        return Response.ok(stream)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Origin, X-Requested-With, Cache-Control")
                .header("Access-Control-Expose-Headers", "Content-Type, Cache-Control")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Max-Age", "86400")
                .header("Cache-Control", "no-cache")
                .header("Connection", "keep-alive")
                .header("X-Accel-Buffering", "no")
                .build();
    }

    // ✅ Manejar OPTIONS para /en-vivo (preflight SSE)
    @OPTIONS
    @Path("/en-vivo")
    public Response optionsEnVivo() {
        return Response.ok()
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Origin, X-Requested-With, Cache-Control")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Max-Age", "86400")
                .build();
    }

    // ✅ Manejar OPTIONS general (preflight)
    @OPTIONS
    public Response options() {
        return Response.ok()
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Origin, X-Requested-With, Cache-Control")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Max-Age", "86400")
                .build();
    }

    // ✅ Manejar OPTIONS para DELETE /{id} (preflight)
    @OPTIONS
    @Path("/{id}")
    public Response optionsDelete() {
        return Response.ok()
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Origin, X-Requested-With, Cache-Control")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Max-Age", "86400")
                .build();
    }

    // ✅ Manejar OPTIONS para /limpiar (preflight)
    @OPTIONS
    @Path("/limpiar")
    public Response optionsLimpiar() {
        return Response.ok()
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                .header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Origin, X-Requested-With, Cache-Control")
                .header("Access-Control-Allow-Credentials", "true")
                .header("Access-Control-Max-Age", "86400")
                .build();
    }

    // ✅ DELETE /{id} con CORS manual (reactivo)
    @DELETE
    @Path("/{id}")
    @WithTransaction
    public Uni<Response> eliminarSismo(@PathParam("id") Long id) {
        return Sismo.deleteById(id)
                .onItem().transform(eliminado -> {
                    if (eliminado) {
                        return Response.ok()
                                .header("Access-Control-Allow-Origin", "*")
                                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                                .header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Origin, X-Requested-With")
                                .build();
                    } else {
                        return Response.status(Response.Status.NOT_FOUND)
                                .header("Access-Control-Allow-Origin", "*")
                                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                                .header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Origin, X-Requested-With")
                                .build();
                    }
                });
    }

    // ✅ DELETE /limpiar (ELIMINA TODOS LOS SISMOS)
    @DELETE
    @Path("/limpiar")
    @WithTransaction
    public Uni<Response> limpiarBaseDeDatos() {
        return Sismo.deleteAll()
                .onItem().transform(cantidad -> {
                    return Response.noContent()
                            .header("Access-Control-Allow-Origin", "*")
                            .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                            .header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Origin, X-Requested-With")
                            .build();
                });
    }

    // ✅ NUEVO: DELETE /limpiar/usuario/{usuarioId} (ELIMINA SOLO LOS SISMOS DE UN USUARIO)
    @DELETE
    @Path("/limpiar/usuario/{usuarioId}")
    @WithTransaction
    public Uni<Response> limpiarSismosPorUsuario(@PathParam("usuarioId") String usuarioId) {
        LOGGER.info("🗑️ Eliminando sismos del usuario: " + usuarioId);
        return Sismo.delete("usuarioId = ?1", usuarioId)
                .onItem().transform(cantidad -> {
                    String mensaje = "Eliminados " + cantidad + " sismos del usuario " + usuarioId;
                    LOGGER.info("✅ " + mensaje);
                    return Response.ok(mensaje)
                            .header("Access-Control-Allow-Origin", "*")
                            .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                            .header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Origin, X-Requested-With")
                            .build();
                });
    }

    // ✅ NUEVO: GET /api/sismos/usuario/{usuarioId} (OBTIENE SISMOS DE UN USUARIO)
    @GET
    @Path("/usuario/{usuarioId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Response> obtenerSismosPorUsuario(@PathParam("usuarioId") String usuarioId) {
        LOGGER.info("📊 Obteniendo sismos del usuario: " + usuarioId);
        return Sismo.find("usuarioId = ?1", usuarioId)
                .list()
                .onItem().transform(sismos -> {
                    return Response.ok(sismos)
                            .header("Access-Control-Allow-Origin", "*")
                            .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                            .header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Origin, X-Requested-With")
                            .header("Access-Control-Expose-Headers", "Content-Type, Cache-Control")
                            .build();
                });
    }
}