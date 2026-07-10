package org.acme.resource;

import io.quarkus.panache.common.Sort;
import io.quarkus.hibernate.reactive.panache.common.WithTransaction;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.acme.model.Sismo;
import org.jboss.resteasy.reactive.RestStreamElementType;
import java.util.List;

@Path("/api/sismos")
@ApplicationScoped
public class SismoResource {

    // El procesador para el streaming en tiempo real
    private final BroadcastProcessor<Sismo> sismoProcessor = BroadcastProcessor.create();

    // 1. MÉTODO PARA EMITIR (Lo que buscan tus otros recursos)
    public void emitirSismo(Sismo sismo) {
        sismoProcessor.onNext(sismo);
    }

    // 2. OBTENER HISTORIAL (REST estándar)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<List<Sismo>> obtenerHistorial() {
        return Sismo.listAll(Sort.by("fechaHora").descending());
    }

    // 3. STREAM EN VIVO (Server-Sent Events)
    @GET
    @Path("/en-vivo")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    @RestStreamElementType(MediaType.APPLICATION_JSON)
    public Multi<Sismo> transmitirSismos() {
        return sismoProcessor;
    }

    // 4. ELIMINAR SISMO INDIVIDUAL
    @DELETE
    @Path("/{id}")
    @WithTransaction
    public Uni<Response> eliminarSismo(@PathParam("id") Long id) {
        return Sismo.deleteById(id)
                .map(eliminado -> eliminado ? Response.ok().build() : Response.status(Response.Status.NOT_FOUND).build());
    }

    // 5. LIMPIAR TODA LA BASE DE DATOS
    @DELETE
    @WithTransaction
    public Uni<Response> limpiarBaseDeDatos() {
        return Sismo.deleteAll()
                .map(cantidad -> Response.noContent().build());
    }
}