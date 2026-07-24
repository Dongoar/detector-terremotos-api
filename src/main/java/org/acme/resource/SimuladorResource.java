package org.acme.resource;

import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.acme.model.Sismo;
import org.acme.service.SismoEventBus;
import org.acme.service.TelegramClient;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
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
    public Uni<Map<String, Object>> simularTerremoto(@QueryParam("tipo") String tipo, @QueryParam("temprana") String temprana) {
        Sismo sismoFalso = new Sismo();

        if ("igp".equalsIgnoreCase(tipo)) {
            sismoFalso.usgsId = "IGP_SIM_" + UUID.randomUUID().toString();
            sismoFalso.magnitud = 4.5;
            sismoFalso.lugar = "Simulación IGP - 15 km al SE de Arequipa, Perú";
            sismoFalso.fechaHora = LocalDateTime.now();
            sismoFalso.latitud = -16.0;
            sismoFalso.longitud = -71.5;
            sismoFalso.alertaCritica = false;
            sismoFalso.usuarioId = "sistema";
        } else {
            sismoFalso.usgsId = "SIM_" + UUID.randomUUID().toString();
            sismoFalso.magnitud = 8;
            sismoFalso.lugar = "12 km SE of Chivay, Arequipa, Peru";
            sismoFalso.fechaHora = LocalDateTime.now();
            sismoFalso.latitud = -15.6383;
            sismoFalso.longitud = -71.5905;
            sismoFalso.alertaCritica = true;
            sismoFalso.usuarioId = "sistema";
        }

        // ✅ DECLARAR LA VARIABLE
        final boolean esAlertaTemprana = "si".equalsIgnoreCase(temprana);

        // ✅ Usar el mismo texto de alerta temprana que en EarthquakeMonitor
        final String mensajeTelegram;
        if (esAlertaTemprana) {
            mensajeTelegram = "🚨🚨 <b>¡ALERTA TEMPRANA!</b> 🚨🚨\n\n" +
                    "🔴 <b>Posible sismo en curso</b>\n" +
                    "📍 <b>Ubicación:</b> " + sismoFalso.lugar + "\n" +
                    "📊 <b>Magnitud:</b> " + String.format("%.1f", sismoFalso.magnitud) + "\n" +
                    "⏱️ <b>Detección:</b> en segundos\n\n" +
                    "⚠️ <i>Busca refugio inmediatamente</i>\n\n" +
                    "⚠️ <i>Esta es una SIMULACIÓN de alerta temprana</i>";
        } else {
            String fechaFormateada = sismoFalso.fechaHora.format(dateFormatter);
            mensajeTelegram = "🔴🔴 <b>⚠️ SIMULACIÓN SÍSMICA</b> 🔴🔴\n\n" +
                    "📊 <b>Magnitud:</b> " + String.format("%.1f", sismoFalso.magnitud) + "\n" +
                    "📍 <b>Ubicación:</b> " + sismoFalso.lugar + "\n" +
                    "📅 <b>Fecha y Hora:</b> " + fechaFormateada + "\n" +
                    "🗺️ <b>Coordenadas:</b>\n" +
                    "   • Latitud: " + String.format("%.4f", sismoFalso.latitud) + "\n" +
                    "   • Longitud: " + String.format("%.4f", sismoFalso.longitud) + "\n\n" +
                    "🔗 <a href='https://www.google.com/maps?q=" + sismoFalso.latitud + "," + sismoFalso.longitud + "'>Ver en Google Maps</a>\n\n" +
                    "⚠️ <i>Esta es una SIMULACIÓN, no un evento real</i>";
        }

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
                .map(target -> {
                    // ✅ Devolver el sismo con un flag de alerta temprana
                    Map<String, Object> response = new HashMap<>();
                    response.put("sismo", sismoFalso);
                    response.put("alertaTemprana", esAlertaTemprana);
                    return response;
                });
    }

    @GET
    @Path("/test-alerta-temprana")
    @Produces(MediaType.TEXT_PLAIN)
    public Uni<String> testAlertaTemprana() {
        LOGGER.info("🧪 Probando alerta temprana...");
        
        String mensaje = "🚨🚨 <b>¡ALERTA TEMPRANA!</b> 🚨🚨\n\n" +
                "🔴 <b>Posible sismo en curso</b>\n" +
                "📍 <b>Ubicación:</b> 12 km SE of Chivay, Arequipa, Peru\n" +
                "📊 <b>Magnitud:</b> 8.0\n" +
                "⏱️ <b>Detección:</b> en segundos\n\n" +
                "⚠️ <i>Busca refugio inmediatamente</i>\n\n" +
                "⚠️ <i>Esta es una PRUEBA de alerta temprana</i>";

        return telegramClient.enviarAlerta(botToken, chatId, mensaje, "HTML")
                .onItem().invoke(res -> LOGGER.info("✅ Alerta temprana de prueba enviada a Telegram"))
                .onFailure().invoke(err -> LOGGER.severe("❌ Error: " + err.getMessage()))
                .onFailure().recoverWithItem("Error al enviar alerta temprana");
    }
}
