package org.acme.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.acme.model.Sismo;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import org.acme.service.SismoEventBus; // <--- ESTO ES LO QUE TE FALTA

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@ApplicationScoped
public class EarthquakeMonitor {

    private static final Logger LOGGER = Logger.getLogger(EarthquakeMonitor.class.getName());
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    @RestClient
    EarthquakeClient earthquakeClient;

    @Inject
    @RestClient
    TelegramClient telegramClient;

    // Inyectamos el bus de eventos en lugar del recurso para evitar dependencias circulares
    @Inject
    SismoEventBus eventBus;

    @ConfigProperty(name = "telegram.bot.token")
    String botToken;

    @ConfigProperty(name = "telegram.chat.id")
    String chatId;

    @WithSession
    @Scheduled(every = "30s")
    public Uni<Void> checkSeismicActivity() {
        LOGGER.info("🔄 Escaneando actividad sísmica global en tiempo real...");

        return earthquakeClient.getRecentEarthquakes("geojson", 2.5)
                .onItem().transformToUni(this::parseAndProcessSismos)
                .onItem().invoke(success -> LOGGER.info("✅ Escaneo de actividad sísmica finalizado."))
                .onFailure().invoke(failure -> LOGGER.severe("❌ Error en el pipeline: " + failure.getMessage()))
                .onFailure().recoverWithNull();
    }

    private Uni<Void> parseAndProcessSismos(String jsonString) {
        List<Sismo> sismosFiltrados = new ArrayList<>();
        try {
            JsonNode root = objectMapper.readTree(jsonString);
            JsonNode features = root.get("features");

            if (features != null && features.isArray()) {
                for (JsonNode feature : features) {
                    if (!feature.has("id") || !feature.has("properties") || !feature.has("geometry")) continue;

                    String usgsId = feature.get("id").asText();
                    JsonNode properties = feature.get("properties");
                    JsonNode geometry = feature.get("geometry");

                    if (!geometry.has("coordinates") || geometry.get("coordinates").size() < 2) continue;
                    JsonNode coordinates = geometry.get("coordinates");

                    double mag = properties.has("mag") && !properties.get("mag").isNull() ? properties.get("mag").asDouble() : 0.0;
                    String place = properties.has("place") ? properties.get("place").asText() : "Ubicación desconocida";
                    long timeMs = properties.has("time") ? properties.get("time").asLong() : 0L;

                    long tiempoActual = System.currentTimeMillis();
                    if ((tiempoActual - timeMs) > 300000) continue;

                    double longitud = coordinates.get(0).asDouble();
                    double latitud = coordinates.get(1).asDouble();

                    String lugarLower = place.toLowerCase();
                    if (mag >= 4.5 || lugarLower.contains("peru") || lugarLower.contains("chile")) {
                        Sismo sismo = new Sismo();
                        sismo.usgsId = usgsId;
                        sismo.magnitud = mag;
                        sismo.lugar = place;
                        sismo.fechaHora = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeMs), ZoneId.systemDefault());
                        sismo.latitud = latitud;
                        sismo.longitud = longitud;
                        sismo.alertaCritica = mag >= 5.5;
                        sismosFiltrados.add(sismo);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.severe("❌ Error crítico en el parseo del JSON: " + e.getMessage());
            return Uni.createFrom().failure(e);
        }

        if (sismosFiltrados.isEmpty()) return Uni.createFrom().voidItem();

        return Multi.createFrom().iterable(sismosFiltrados)
                .onItem().transformToUniAndConcatenate(this::procesarSismoIndividual)
                .collect().asList()
                .map(list -> null);
    }

    private Uni<Void> procesarSismoIndividual(Sismo sismo) {
        return Sismo.find("usgsId", sismo.usgsId).firstResult()
                .onItem().transformToUni(existing -> {
                    if (existing != null) return Uni.createFrom().voidItem();

                    String mensaje = "🚨 <b>ALERTA SÍSMICA</b> 🚨\n\n" +
                            "• <b>Magnitud:</b> " + sismo.magnitud + "\n" +
                            "• <b>Lugar:</b> " + sismo.lugar;

                    return Panache.withTransaction(sismo::persist)
                            .onItem().invoke(p -> {
                                // 📢 AQUÍ ES DONDE SE EMITE EL EVENTO HACIA EL FRONTEND
                                eventBus.emitir(sismo);
                                LOGGER.info("📢 Sismo emitido al stream en tiempo real: " + sismo.lugar);
                            })
                            .onItem().transformToUni(p ->
                                    telegramClient.enviarAlerta(botToken, chatId, mensaje, "HTML")
                            )
                            .map(target -> null);
                });
    }
}