package org.acme.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.acme.model.Sismo;
import io.quarkus.hibernate.reactive.panache.Panache;
import io.quarkus.hibernate.reactive.panache.common.WithSession;
import org.acme.service.SismoEventBus;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@ApplicationScoped
public class EarthquakeMonitor {

    private static final Logger LOGGER = Logger.getLogger(EarthquakeMonitor.class.getName());
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    @Inject
    @RestClient
    EarthquakeClient earthquakeClient;

    @Inject
    @RestClient
    TelegramClient telegramClient;

    @Inject
    SismoEventBus eventBus;

    @ConfigProperty(name = "telegram.bot.token")
    String botToken;

    @ConfigProperty(name = "telegram.chat.id")
    String chatId;

    // ✅ Método de inicialización forzada en Cloud Run
    @PostConstruct
    public void init() {
        LOGGER.info("🚀 EarthquakeMonitor iniciado correctamente");
        LOGGER.info("⏰ Scheduler configurado para ejecutarse cada 30 segundos");
        LOGGER.info("📡 Conectando a USGS: " +
                (System.getenv("QUARKUS_PROFILE") != null ? "PRODUCCIÓN" : "DESARROLLO"));
    }

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

                    // ✅ CAMBIO 1: Aumentar ventana de tiempo a 24 HORAS (86,400,000 ms)
                    if ((tiempoActual - timeMs) > 86400000) continue;

                    double longitud = coordinates.get(0).asDouble();
                    double latitud = coordinates.get(1).asDouble();

                    String lugarLower = place.toLowerCase();

                    // ✅ CAMBIO 2: Reducir filtro de magnitud a 2.5
                    if (mag >= 2.5 || lugarLower.contains("peru") || lugarLower.contains("chile")) {
                        Sismo sismo = new Sismo();
                        sismo.usgsId = usgsId;
                        sismo.magnitud = mag;
                        sismo.lugar = place;
                        sismo.fechaHora = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeMs), ZoneId.systemDefault());
                        sismo.latitud = latitud;
                        sismo.longitud = longitud;
                        sismo.alertaCritica = mag >= 5.5;
                        sismo.usuarioId = "sistema";

                        sismosFiltrados.add(sismo);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.severe("❌ Error crítico en el parseo del JSON: " + e.getMessage());
            return Uni.createFrom().failure(e);
        }

        if (sismosFiltrados.isEmpty()) {
            LOGGER.info("📭 No se encontraron sismos que cumplan los filtros.");
            return Uni.createFrom().voidItem();
        }

        LOGGER.info("📊 Procesando " + sismosFiltrados.size() + " sismos encontrados...");

        return Multi.createFrom().iterable(sismosFiltrados)
                .onItem().transformToUniAndConcatenate(this::procesarSismoIndividual)
                .collect().asList()
                .map(list -> null);
    }

    private Uni<Void> procesarSismoIndividual(Sismo sismo) {
        return Sismo.find("usgsId", sismo.usgsId).firstResult()
                .onItem().transformToUni(existing -> {
                    if (existing != null) {
                        LOGGER.info("⏭️ Sismo duplicado ignorado: " + sismo.usgsId);
                        return Uni.createFrom().voidItem();
                    }

                    // ✅ Formatear fecha
                    String fechaFormateada = sismo.fechaHora.format(dateFormatter);

                    // ✅ Construir el mensaje BASE
                    String mensajeBase = "📊 <b>Magnitud:</b> " + String.format("%.1f", sismo.magnitud) + "\n" +
                            "📍 <b>Ubicación:</b> " + sismo.lugar + "\n" +
                            "📅 <b>Fecha y Hora:</b> " + fechaFormateada + "\n" +
                            "🗺️ <b>Coordenadas:</b>\n" +
                            "   • Latitud: " + String.format("%.4f", sismo.latitud) + "\n" +
                            "   • Longitud: " + String.format("%.4f", sismo.longitud) + "\n\n" +
                            "🔗 <a href='https://www.google.com/maps?q=" + sismo.latitud + "," + sismo.longitud + "'>Ver en Google Maps</a>";

                    // ✅ Construir el mensaje FINAL (con o sin alerta crítica)
                    final String mensajeFinal = sismo.alertaCritica ?
                            "🔴🔴 <b>¡ALERTA CRÍTICA!</b> 🔴🔴\n\n🚨 <b>ALERTA SÍSMICA</b> 🚨\n\n" + mensajeBase :
                            "🚨 <b>ALERTA SÍSMICA</b> 🚨\n\n" + mensajeBase;

                    return Panache.withTransaction(sismo::persist)
                            .onItem().invoke(p -> {
                                eventBus.emitir(sismo);
                                LOGGER.info("📢 Sismo emitido al stream: " + sismo.lugar + " (M" + sismo.magnitud + ")");
                            })
                            .onItem().transformToUni(p ->
                                    telegramClient.enviarAlerta(botToken, chatId, mensajeFinal, "HTML")
                                            .onItem().invoke(res -> LOGGER.info("📲 Alerta enviada a Telegram: " + sismo.lugar))
                                            .onFailure().invoke(err -> LOGGER.warning("⚠️ Error enviando a Telegram: " + err.getMessage()))
                                            .onFailure().recoverWithItem(() -> null)
                            )
                            .onItem().transformToUni(result -> Uni.createFrom().voidItem());
                });
    }
}