package org.acme.resource;

import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.acme.model.Sismo;
import org.acme.service.TelegramClient;

import java.time.LocalDateTime;
import java.util.logging.Logger;

@Path("/api/simular")
public class SimuladorResource {

    private static final Logger LOGGER = Logger.getLogger(SimuladorResource.class.getName());

    @Inject
    @RestClient
    TelegramClient telegramClient;

    @Inject
    SismoResource sismoResource; // 👈 1. Inyectamos tu recurso de tiempo real

    @ConfigProperty(name = "telegram.bot.token")
    String botToken;

    @ConfigProperty(name = "telegram.chat.id")
    String chatId;

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Sismo> simularTerremoto() {
        Sismo sismoFalso = new Sismo();
        sismoFalso.magnitud = 8;
        sismoFalso.lugar = "12 km SE of Chivay, Arequipa, Peru";
        sismoFalso.fechaHora = LocalDateTime.now();
        sismoFalso.latitud = -15.6383;
        sismoFalso.longitud = -71.5905;
        sismoFalso.alertaCritica = true;

        String lugarLimpio = sismoFalso.lugar.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");

        String mensajeTelegram = "🚨 <b>SIMULACION SISMICA DETECTADA</b> 🚨\n\n" +
                "• <b>Magnitud:</b> " + sismoFalso.magnitud + "\n" +
                "• <b>Ubicacion:</b> " + lugarLimpio + "\n" +
                "• <b>Fecha:</b> " + sismoFalso.fechaHora.toString() + "\n" +
                "• <b>Coords:</b> " + sismoFalso.latitud + ", " + sismoFalso.longitud;

        return Panache.withTransaction(sismoFalso::persist)
                .onItem().invoke(persistedItem -> {
                    // 👈 CAMBIO: Emitimos el evento a la interfaz en tiempo real
                    // Asegúrate de que este método en sismoResource llame a tu Emitter
                    sismoResource.emitirSismo((Sismo) persistedItem);
                })
                .onItem().transformToUni(persistedItem ->
                        telegramClient.enviarAlerta(botToken, chatId, mensajeTelegram, "HTML")
                                .onItem().invoke(res -> LOGGER.info("📲 Alerta simulada enviada."))
                                .onFailure().invoke(err -> LOGGER.severe("❌ Error Telegram: " + err.getMessage()))
                                .onFailure().recoverWithItem(err -> "")
                )
                .map(target -> sismoFalso);
    }
}