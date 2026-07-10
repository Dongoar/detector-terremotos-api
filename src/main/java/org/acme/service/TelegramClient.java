package org.acme.service;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import io.smallrye.mutiny.Uni;

/**
 * Cliente REST para interactuar con la API de Telegram.
 * Se utiliza @Consumes(MediaType.APPLICATION_FORM_URLENCODED) para enviar
 * los datos en el cuerpo de la petición, evitando conflictos con los parámetros.
 */
@RegisterRestClient(baseUri = "https://api.telegram.org")
public interface TelegramClient {

    @POST
    @Path("/bot{token}/sendMessage")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    Uni<String> enviarAlerta(
            @PathParam("token") String token,
            @FormParam("chat_id") String chatId,
            @FormParam("text") String texto,
            @FormParam("parse_mode") String parseMode
    );
}