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

    // ✅ Variables para detección temprana
    private Sismo ultimoSismo = null;
    private long ultimoTiempoDeteccion = 0;

    @Inject
    @RestClient
    EarthquakeClient earthquakeClient;

    @Inject
    @RestClient
    TelegramClient telegramClient;

    @Inject
    @RestClient
    IgpClient igpClient;

    @Inject
    SismoEventBus eventBus;

    @ConfigProperty(name = "telegram.bot.token")
    String botToken;

    @ConfigProperty(name = "telegram.chat.id")
    String chatId;

    @PostConstruct
    public void init() {
        LOGGER.info("🚀 EarthquakeMonitor iniciado correctamente");
        LOGGER.info("⏰ Scheduler configurado para ejecutarse cada 3 segundos");
        LOGGER.info("📡 Conectando a USGS: " +
                (System.getenv("QUARKUS_PROFILE") != null ? "PRODUCCIÓN" : "DESARROLLO"));
        LOGGER.info("📡 Conectando a IGP (Perú)");
    }

    @WithSession
    @Scheduled(every = "3s")
    public Uni<Void> checkSeismicActivity() {
        LOGGER.info("🔄 Escaneo ultrarrápido (3s)...");

        Uni<Void> usgsUni = earthquakeClient.getRecentEarthquakes("geojson", 2.5)
                .onItem().transformToUni(this::parseAndProcessSismos);

        Uni<Void> igpUni = igpClient.getUltimoSismo()
                .onItem().transformToUni(this::parseAndProcessIgpSismo)
                .onFailure().recoverWithItem(() -> {
                    LOGGER.warning("⚠️ Error consultando IGP, continuando con USGS");
                    return null;
                });

        return Uni.combine().all().unis(usgsUni, igpUni)
                .with(ignored -> {
                    LOGGER.info("✅ Escaneo completado (3s)");
                    return null;
                })
                .onItem().transformToUni(ignored -> limpiarSismosViejos())
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
                    if ((tiempoActual - timeMs) > 86400000) continue;

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
            LOGGER.info("📭 No se encontraron sismos que cumplan los filtros (USGS).");
            return Uni.createFrom().voidItem();
        }

        LOGGER.info("📊 Procesando " + sismosFiltrados.size() + " sismos encontrados (USGS)...");
        return Multi.createFrom().iterable(sismosFiltrados)
                .onItem().transformToUniAndConcatenate(this::procesarSismoIndividual)
                .collect().asList()
                .map(list -> null);
    }

    private Uni<Void> parseAndProcessIgpSismo(String jsonString) {
        try {
            JsonNode root = objectMapper.readTree(jsonString);
            if (root == null || !root.has("magnitud")) {
                LOGGER.info("📭 No hay sismo reciente del IGP");
                return Uni.createFrom().voidItem();
            }

            double mag = root.has("magnitud") ? root.get("magnitud").asDouble() : 0;
            String lugar = root.has("referencia") ? root.get("referencia").asText() : "Perú";
            double lat = root.has("latitud") ? root.get("latitud").asDouble() : 0;
            double lon = root.has("longitud") ? root.get("longitud").asDouble() : 0;
            String fechaHoraStr = root.has("fecha_hora") ? root.get("fecha_hora").asText() : null;
            String intensidad = root.has("intensidades") ? root.get("intensidades").asText() : "No reportada";
            String codigo = root.has("codigo") ? root.get("codigo").asText() : "IGP_" + System.currentTimeMillis();

            LocalDateTime fechaHora;
            if (fechaHoraStr != null) {
                fechaHora = LocalDateTime.parse(fechaHoraStr, DateTimeFormatter.ISO_DATE_TIME);
            } else {
                fechaHora = LocalDateTime.now();
            }

            Sismo sismo = new Sismo();
            sismo.usgsId = codigo;
            sismo.magnitud = mag;
            sismo.lugar = lugar + " (IGP)";
            sismo.fechaHora = fechaHora;
            sismo.latitud = lat;
            sismo.longitud = lon;
            sismo.alertaCritica = mag >= 5.5;
            sismo.usuarioId = "sistema";

            return sismoYaExiste(sismo)
                    .onItem().transformToUni(existe -> {
                        if (existe) {
                            LOGGER.info("⏭️ Sismo IGP duplicado ignorado: " + lugar + " M" + mag);
                            return Uni.createFrom().voidItem();
                        }
                        LOGGER.info("🌍 Sismo IGP detectado: " + lugar + " M" + mag + " (Intensidad: " + intensidad + ")");
                        return procesarSismoIndividual(sismo);
                    });
        } catch (Exception e) {
            LOGGER.severe("❌ Error procesando sismo del IGP: " + e.getMessage());
            return Uni.createFrom().voidItem();
        }
    }

    private Uni<Boolean> sismoYaExiste(Sismo sismo) {
        LocalDateTime inicio = sismo.fechaHora.minusMinutes(5);
        LocalDateTime fin = sismo.fechaHora.plusMinutes(5);
        return Sismo.find("lugar = ?1 AND fechaHora BETWEEN ?2 AND ?3",
                        sismo.lugar, inicio, fin)
                .firstResult()
                .onItem().transform(existing -> existing != null);
    }

    private Uni<Void> procesarSismoIndividual(Sismo sismo) {
        return Sismo.find("usgsId", sismo.usgsId).firstResult()
                .onItem().transformToUni(existing -> {
                    if (existing != null) {
                        LOGGER.info("⏭️ Sismo duplicado ignorado: " + sismo.usgsId);
                        return Uni.createFrom().voidItem();
                    }

                    // ✅ DETECCIÓN TEMPRANA
                    long ahora = System.currentTimeMillis();
                    if (ultimoSismo != null) {
                        long diferencia = ahora - ultimoTiempoDeteccion;
                        LOGGER.info("⏱️ Último sismo hace " + (diferencia / 1000) + " segundos");
                        if (diferencia < 30000) {
                            LOGGER.info("🚨 ¡ALERTA TEMPRANA! Nuevo sismo detectado en " + (diferencia / 1000) + " segundos");
                            enviarAlertaTemprana(sismo);
                        }
                    }
                    ultimoSismo = sismo;
                    ultimoTiempoDeteccion = ahora;

                    String fechaFormateada = sismo.fechaHora.format(dateFormatter);
                    String mensajeBase = "📊 <b>Magnitud:</b> " + String.format("%.1f", sismo.magnitud) + "\n" +
                            "📍 <b>Ubicación:</b> " + sismo.lugar + "\n" +
                            "📅 <b>Fecha y Hora:</b> " + fechaFormateada + "\n" +
                            "🗺️ <b>Coordenadas:</b>\n" +
                            "   • Latitud: " + String.format("%.4f", sismo.latitud) + "\n" +
                            "   • Longitud: " + String.format("%.4f", sismo.longitud) + "\n\n" +
                            "🔗 <a href='https://www.google.com/maps?q=" + sismo.latitud + "," + sismo.longitud + "'>Ver en Google Maps</a>";

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

    private void enviarAlertaTemprana(Sismo sismo) {
        String mensaje = "🚨🚨 <b>¡ALERTA TEMPRANA!</b> 🚨🚨\n\n" +
                "🔴 <b>Posible sismo en curso</b>\n" +
                "📍 <b>Ubicación:</b> " + sismo.lugar + "\n" +
                "📊 <b>Magnitud:</b> " + String.format("%.1f", sismo.magnitud) + "\n" +
                "⏱️ <b>Detección:</b> en segundos\n\n" +
                "⚠️ <i>Busca refugio inmediatamente</i>";

        telegramClient.enviarAlerta(botToken, chatId, mensaje, "HTML")
                .onItem().invoke(res -> LOGGER.info("📲 ALERTA TEMPRANA enviada a Telegram"))
                .onFailure().invoke(err -> LOGGER.warning("⚠️ Error enviando alerta temprana: " + err.getMessage()))
                .subscribe().with(
                    success -> {},
                    failure -> {}
                );
    }

    private Uni<Void> limpiarSismosViejos() {
        LocalDateTime hace7Dias = LocalDateTime.now().minusDays(7);
        return Panache.withTransaction(() ->
            Sismo.delete("fechaHora < ?1", hace7Dias)
                .onItem().invoke(cantidad -> {
                    if (cantidad > 0) {
                        LOGGER.info("🗑️ Eliminados " + cantidad + " sismos antiguos (>7 días)");
                    }
                })
                .onItem().transformToUni(result -> Uni.createFrom().voidItem())
        );
    }
}
