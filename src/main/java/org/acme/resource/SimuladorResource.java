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
import org.acme.service.SismoEventBus;
import org.acme.service.TelegramClient;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.logging.Logger;

@Path("/api/simular")
public class SimuladorResource {

    private static final Logger LOGGER = Logger.getLogger(SimuladorResource.class.getName());
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    @Inject
    @RestClient
    TelegramClient telegramClient;

    @Inject
    SismoEventBus eventBus;

    @ConfigProperty(name = "telegram.bot.token")
    String botToken;

    @ConfigProperty(name = "telegram.chat.id")
    String chatId;

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Sismo> simularTerremoto() {
        Sismo sismoFalso = new Sismo();

        sismoFalso.usgsId = "SIM_" + UUID.randomUUID().toString();
        sismoFalso.magnitud = 8;
        sismoFalso.lugar = "12 km SE of Chivay, Arequipa, Peru";
        sismoFalso.fechaHora = LocalDateTime.now();
        sismoFalso.latitud = -15.6383;
        sismoFalso.longitud = -71.5905;
        sismoFalso.alertaCritica = true;
        sismoFalso.usuarioId = "sistema"; // ✅ AGREGADO

        // ✅ Formatear fecha y hora
        String fechaFormateada = sismoFalso.fechaHora.format(dateFormatter);

        // ✅ Construir mensaje con todos los detalles
        final String mensajeTelegram = "🔴🔴 <b>⚠️ SIMULACIÓN SÍSMICA</b> 🔴🔴\n\n" +
                "📊 <b>Magnitud:</b> " + String.format("%.1f", sismoFalso.magnitud) + "\n" +
                "📍 <b>Ubicación:</b> " + sismoFalso.lugar + "\n" +
                "📅 <b>Fecha y Hora:</b> " + fechaFormateada + "\n" +
                "🗺️ <b>Coordenadas:</b>\n" +
                "   • Latitud: " + String.format("%.4f", sismoFalso.latitud) + "\n" +
                "   • Longitud: " + String.format("%.4f", sismoFalso.longitud) + "\n\n" +
                "🔗 <a href='https://www.google.com/maps?q=" + sismoFalso.latitud + "," + sismoFalso.longitud + "'>Ver en Google Maps</a>\n\n" +
                "⚠️ <i>Esta es una SIMULACIÓN, no un evento real</i>";

        return Panache.withTransaction(sismoFalso::persist)
                .onItem().invoke(persistedItem -> {
                    eventBus.emitir(sismoFalso);
                    LOGGER.info("📢 Sismo simulado emitido al stream: " + sismoFalso.lugar + " (ID: " + sismoFalso.usgsId + ")");
                })
                .onItem().transformToUni(persistedItem ->
                        telegramClient.enviarAlerta(botToken, chatId, mensajeTelegram, "HTML")
                                .onItem().invoke(res -> LOGGER.info("📲 Alerta simulada enviada a Telegram"))
                                .onFailure().invoke(err -> LOGGER.severe("❌ Error Telegram: " + err.getMessage()))
                                .onFailure().recoverWithItem(err -> "")
                )
                .map(target -> sismoFalso);
    }
}