package org.acme.service;

import io.quarkus.scheduler.Scheduled;
import io.quarkus.websockets.next.WebSocketConnector;
import io.quarkus.websockets.next.WebSocketClientConnection;
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

import java.net.URI;
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

    // ============================================================
    // 🌍 CLIENTES EXISTENTES
    // ============================================================

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

    // ============================================================
    // 🆕 CLIENTE WEBSOCKET
    // ============================================================

    @Inject
    IgpWebSocketClient igpWebSocketClient;

    @Inject
    WebSocketConnector<IgpWebSocketClient> wsConnector;

    @ConfigProperty(name = "telegram.bot.token")
    String botToken;

    @ConfigProperty(name = "telegram.chat.id")
    String chatId;

    @ConfigProperty(name = "igp.websocket.url")
    String igpWebSocketUrl;

    @PostConstruct
    public void init() {
        LOGGER.info("🚀 EarthquakeMonitor iniciado correctamente");
        LOGGER.info("⏰ Scheduler configurado para ejecutarse cada 3 segundos");
        LOGGER.info("📡 Conectando a USGS: " +
                (System.getenv("QUARKUS_PROFILE") != null ? "PRODUCCIÓN" : "DESARROLLO"));
        LOGGER.info("📡 Conectando a IGP (Perú) - REST + WebSocket");

        // ✅ CONECTAR WEBSOCKET DEL IGP
        conectarWebSocketIgp();
    }

    /**
     * Conecta al WebSocket del IGP en tiempo real
     */
    private void conectarWebSocketIgp() {
        try {
            if (igpWebSocketUrl == null || igpWebSocketUrl.isEmpty()) {
                LOGGER.warning("⚠️ URL del WebSocket IGP no configurada");
                return;
            }

            LOGGER.info("🔌 Conectando WebSocket del IGP: " + igpWebSocketUrl);

            // ✅ CONEXIÓN REACTIVA - NO BLOQUEANTE
            wsConnector
                    .baseUri(URI.create(igpWebSocketUrl))
                    .addHeader("Sec-WebSocket-Protocol", "graphql-ws")
                    .connect()
                    .subscribe().with(
                            connection -> {
                                LOGGER.info("✅ WebSocket del IGP conectado exitosamente!");
                                enviarMensajesIniciales(connection);
                            },
                            failure -> {
                                LOGGER.warning("⚠️ Error conectando WebSocket del IGP: " + failure.getMessage());
                                LOGGER.warning("⚠️ Continuando con REST polling como fallback");
                            }
                    );

        } catch (Exception e) {
            LOGGER.warning("⚠️ Error al iniciar conexión WebSocket: " + e.getMessage());
            LOGGER.warning("⚠️ Continuando con REST polling como fallback");
        }
    }

    /**
     * Envía mensajes de inicialización al WebSocket
     */
    private void enviarMensajesIniciales(WebSocketClientConnection connection) {
        try {
            // ✅ PASO 1: connection_init
            String initMessage = """
                {
                    "type": "connection_init",
                    "payload": {}
                }
            """;
            connection.sendText(initMessage);
            LOGGER.info("📤 Mensaje 'connection_init' enviado");

            // ✅ Esperar un poco para que AWS AppSync procese el init
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // ✅ PASO 2: Suscripción GraphQL - Formato 1
            String subscribeMessage1 = """
                {
                    "type": "subscribe",
                    "id": "1",
                    "payload": {
                        "query": "subscription { newEarthquake { id magnitud lugar latitud longitud profundidad fechaHora } }"
                    }
                }
            """;
            connection.sendText(subscribeMessage1);
            LOGGER.info("📤 Suscripción GraphQL (newEarthquake) enviada");

            // ✅ PASO 3: Suscripción GraphQL - Formato 2 (alternativo)
            String subscribeMessage2 = """
                {
                    "type": "subscribe",
                    "id": "2",
                    "payload": {
                        "query": "subscription { sismo { id magnitud referencia latitud longitud profundidad fecha_hora } }"
                    }
                }
            """;
            // Descomentar si el primer formato no funciona
            // connection.sendText(subscribeMessage2);
            // LOGGER.info("📤 Suscripción GraphQL (sismo) enviada");

        } catch (Exception e) {
            LOGGER.warning("⚠️ Error enviando mensajes iniciales: " + e.getMessage());
        }
    }

    @WithSession
    @Scheduled(every = "3s")
    public Uni<Void> checkSeismicActivity() {
        LOGGER.fine("🔄 Escaneo sísmico (3s)...");

        // 🌍 USGS (Global)
        Uni<Void> usgsUni = earthquakeClient.getRecentEarthquakes("geojson", 2.5)
                .onItem().transformToUni(this::parseAndProcessSismos)
                .onFailure().recoverWithItem(() -> {
                    LOGGER.warning("⚠️ Error USGS, continuando");
                    return null;
                });

        // 🇵🇪 IGP Perú (REST - Fallback)
        Uni<Void> igpUni = igpClient.getUltimoSismo()
                .onItem().transformToUni(this::parseAndProcessIgpSismo)
                .onFailure().recoverWithItem(() -> {
                    LOGGER.fine("⚠️ Error IGP REST, continuando");
                    return null;
                });

        return Uni.combine().all().unis(usgsUni, igpUni)
                .with(ignored -> {
                    LOGGER.fine("✅ Escaneo completado");
                    return null;
                })
                .onItem().transformToUni(ignored -> limpiarSismosViejos())
                .onFailure().invoke(failure -> LOGGER.warning("⚠️ Error en pipeline: " + failure.getMessage()))
                .onFailure().recoverWithNull();
    }

    // ============================================================
    // 🌍 PARSEO - USGS
    // ============================================================

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
                    double profundidad = coordinates.size() > 2 ? coordinates.get(2).asDouble() : 0;

                    String lugarLower = place.toLowerCase();
                    if (mag >= 4.5 || lugarLower.contains("peru") || lugarLower.contains("chile") ||
                            lugarLower.contains("argentina") || lugarLower.contains("ecuador") ||
                            lugarLower.contains("colombia") || lugarLower.contains("bolivia") ||
                            lugarLower.contains("venezuela") || lugarLower.contains("paraguay") ||
                            lugarLower.contains("uruguay") || lugarLower.contains("brasil") ||
                            lugarLower.contains("brazil")) {

                        String pais = detectarPais(place);

                        Sismo sismo = crearSismo(
                                usgsId,
                                mag, place,
                                LocalDateTime.ofInstant(Instant.ofEpochMilli(timeMs), ZoneId.systemDefault()),
                                latitud, longitud, profundidad,
                                pais, "USGS"
                        );
                        sismosFiltrados.add(sismo);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.severe("❌ Error en parseo USGS: " + e.getMessage());
            return Uni.createFrom().voidItem();
        }

        if (sismosFiltrados.isEmpty()) {
            LOGGER.fine("📭 No se encontraron sismos de Sudamérica (USGS)");
            return Uni.createFrom().voidItem();
        }

        LOGGER.info("📊 Procesando " + sismosFiltrados.size() + " sismos de USGS en Sudamérica...");
        return Multi.createFrom().iterable(sismosFiltrados)
                .onItem().transformToUniAndConcatenate(this::guardarSismo)
                .collect().asList()
                .map(list -> null);
    }

    // ============================================================
    // 🇵🇪 PARSEO - IGP REST (FALLBACK)
    // ============================================================

    private Uni<Void> parseAndProcessIgpSismo(String jsonString) {
        try {
            if (jsonString == null || jsonString.trim().isEmpty()) {
                LOGGER.fine("📭 IGP: respuesta vacía");
                return Uni.createFrom().voidItem();
            }

            JsonNode root = objectMapper.readTree(jsonString);
            if (root == null || !root.has("magnitud")) {
                LOGGER.fine("📭 No hay sismo reciente del IGP");
                return Uni.createFrom().voidItem();
            }

            double mag = root.has("magnitud") ? root.get("magnitud").asDouble() : 0;
            String lugar = root.has("referencia") ? root.get("referencia").asText() : "Perú";
            double lat = root.has("latitud") ? root.get("latitud").asDouble() : 0;
            double lon = root.has("longitud") ? root.get("longitud").asDouble() : 0;
            String fechaHoraStr = root.has("fecha_hora") ? root.get("fecha_hora").asText() : null;
            double profundidad = root.has("profundidad") ? root.get("profundidad").asDouble() : 0;
            String codigo = root.has("codigo") ? root.get("codigo").asText() : "IGP_" + System.currentTimeMillis();

            LocalDateTime fechaHora = parseFecha(fechaHoraStr);

            Sismo sismo = crearSismo(
                    codigo,
                    mag, lugar,
                    fechaHora, lat, lon, profundidad,
                    "Perú", "IGP-REST"
            );

            return guardarSismo(sismo);
        } catch (Exception e) {
            LOGGER.warning("⚠️ Error IGP REST: " + e.getMessage());
            return Uni.createFrom().voidItem();
        }
    }

    // ============================================================
    // 🆕 PROCESAR MENSAJE DEL WEBSOCKET
    // ============================================================

    /**
     * Procesa mensajes del WebSocket del IGP en tiempo real
     * Este método es llamado desde IgpWebSocketClient
     */
    public void procesarMensajeWebSocket(String mensaje) {
        try {
            if (mensaje == null || mensaje.trim().isEmpty()) {
                return;
            }

            LOGGER.info("📩 Mensaje WebSocket recibido");

            JsonNode root = objectMapper.readTree(mensaje);

            // ✅ Verificar tipo de mensaje
            String type = root.has("type") ? root.get("type").asText() : "";

            // 🔍 Si es un mensaje de datos (sismo)
            if ("data".equals(type) && root.has("payload")) {
                JsonNode payload = root.get("payload");
                JsonNode data = payload.has("data") ? payload.get("data") : null;

                if (data == null) {
                    LOGGER.fine("📭 Mensaje sin datos");
                    return;
                }

                // Buscar el sismo en diferentes formatos
                JsonNode sismoData = null;

                if (data.has("newEarthquake")) {
                    sismoData = data.get("newEarthquake");
                } else if (data.has("sismo")) {
                    sismoData = data.get("sismo");
                } else if (data.has("earthquake")) {
                    sismoData = data.get("earthquake");
                } else if (data.has("getEarthquake")) {
                    sismoData = data.get("getEarthquake");
                } else if (data.has("magnitud") || data.has("magnitude")) {
                    sismoData = data;
                }

                if (sismoData == null) {
                    LOGGER.fine("📭 No se encontró sismo en el mensaje");
                    return;
                }

                if (!sismoData.has("magnitud") && !sismoData.has("magnitude")) {
                    LOGGER.fine("📭 Mensaje sin magnitud");
                    return;
                }

                // ✅ Extraer datos del sismo
                double mag = sismoData.has("magnitud") ? sismoData.get("magnitud").asDouble() :
                        (sismoData.has("magnitude") ? sismoData.get("magnitude").asDouble() : 0);

                String lugar = sismoData.has("lugar") ? sismoData.get("lugar").asText() :
                        (sismoData.has("referencia") ? sismoData.get("referencia").asText() :
                         (sismoData.has("place") ? sismoData.get("place").asText() : "Perú"));

                double lat = sismoData.has("latitud") ? sismoData.get("latitud").asDouble() :
                        (sismoData.has("latitude") ? sismoData.get("latitude").asDouble() : 0);

                double lon = sismoData.has("longitud") ? sismoData.get("longitud").asDouble() :
                        (sismoData.has("longitude") ? sismoData.get("longitude").asDouble() : 0);

                double profundidad = sismoData.has("profundidad") ? sismoData.get("profundidad").asDouble() :
                        (sismoData.has("depth") ? sismoData.get("depth").asDouble() : 0);

                String id = sismoData.has("id") ? sismoData.get("id").asText() :
                        (sismoData.has("codigo") ? sismoData.get("codigo").asText() :
                         "IGP_WS_" + System.currentTimeMillis());

                String fechaStr = sismoData.has("fecha_hora") ? sismoData.get("fecha_hora").asText() :
                        (sismoData.has("time") ? sismoData.get("time").asText() : null);
                LocalDateTime fechaHora = parseFecha(fechaStr);

                LOGGER.info("🚨 ¡SISMO EN TIEMPO REAL DESDE WEBSOCKET! M" + mag + " - " + lugar);

                // ✅ Crear y guardar el sismo
                Sismo sismo = crearSismo(
                        id,
                        mag, lugar,
                        fechaHora, lat, lon, profundidad,
                        "Perú", "IGP-WS"
                );

                // ✅ Guardar inmediatamente
                guardarSismo(sismo).subscribe().with(
                        success -> LOGGER.info("✅ Sismo desde WebSocket guardado: " + lugar),
                        failure -> LOGGER.warning("⚠️ Error guardando sismo desde WebSocket: " + failure.getMessage())
                );

            } else if ("connection_ack".equals(type)) {
                LOGGER.info("✅ Conexión WebSocket IGP confirmada (connection_ack)");

            } else if ("ka".equals(type)) {
                LOGGER.fine("🔵 Keep-alive del WebSocket IGP");

            } else if ("error".equals(type)) {
                LOGGER.warning("❌ Error en WebSocket IGP: " + root.toString());

            } else {
                LOGGER.fine("📩 Otro tipo de mensaje WebSocket: " + type);
            }

        } catch (Exception e) {
            LOGGER.warning("⚠️ Error procesando mensaje WebSocket: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ============================================================
    // 🛠️ MÉTODOS AUXILIARES
    // ============================================================

    private LocalDateTime parseFecha(String fechaStr) {
        if (fechaStr == null) return LocalDateTime.now();
        try {
            return LocalDateTime.parse(fechaStr, DateTimeFormatter.ISO_DATE_TIME);
        } catch (Exception e) {
            try {
                return LocalDateTime.parse(fechaStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            } catch (Exception e2) {
                return LocalDateTime.now();
            }
        }
    }

    private String detectarPais(String lugar) {
        if (lugar == null) return "Desconocido";
        String lower = lugar.toLowerCase();
        if (lower.contains("peru") || lower.contains("perú")) return "Perú";
        if (lower.contains("chile")) return "Chile";
        if (lower.contains("argentina")) return "Argentina";
        if (lower.contains("ecuador")) return "Ecuador";
        if (lower.contains("colombia")) return "Colombia";
        if (lower.contains("bolivia")) return "Bolivia";
        if (lower.contains("venezuela")) return "Venezuela";
        if (lower.contains("paraguay")) return "Paraguay";
        if (lower.contains("uruguay")) return "Uruguay";
        if (lower.contains("brasil") || lower.contains("brazil")) return "Brasil";
        if (lower.contains("guyana")) return "Guyana";
        if (lower.contains("surinam")) return "Surinam";
        if (lower.contains("guayana") || lower.contains("french")) return "Guayana Francesa";
        return "Desconocido";
    }

    private String obtenerBandera(String pais) {
        if (pais == null) return "🌍";
        switch (pais) {
            case "Perú": return "🇵🇪";
            case "Chile": return "🇨🇱";
            case "Argentina": return "🇦🇷";
            case "Ecuador": return "🇪🇨";
            case "Colombia": return "🇨🇴";
            case "Bolivia": return "🇧🇴";
            case "Venezuela": return "🇻🇪";
            case "Paraguay": return "🇵🇾";
            case "Uruguay": return "🇺🇾";
            case "Brasil": return "🇧🇷";
            default: return "🌍";
        }
    }

    private Sismo crearSismo(String id, double magnitud, String lugar,
                             LocalDateTime fechaHora, double lat, double lon,
                             double profundidad, String pais, String fuente) {
        Sismo sismo = new Sismo();
        sismo.usgsId = id;
        sismo.magnitud = magnitud;
        sismo.lugar = lugar;
        sismo.fechaHora = fechaHora;
        sismo.latitud = lat;
        sismo.longitud = lon;
        sismo.profundidad = profundidad;
        sismo.alertaCritica = magnitud >= 5.5;
        sismo.usuarioId = "sistema";
        sismo.pais = pais;
        sismo.fuente = fuente;
        return sismo;
    }

    private Uni<Boolean> sismoYaExiste(Sismo sismo) {
        return Sismo.find("usgsId", sismo.usgsId).firstResult()
                .onItem().transformToUni(existente -> {
                    if (existente != null) {
                        return Uni.createFrom().item(true);
                    }
                    LocalDateTime inicio = sismo.fechaHora.minusMinutes(5);
                    LocalDateTime fin = sismo.fechaHora.plusMinutes(5);
                    return Sismo.find("lugar = ?1 AND fechaHora BETWEEN ?2 AND ?3",
                                    sismo.lugar, inicio, fin)
                            .firstResult()
                            .onItem().transform(existing -> existing != null);
                });
    }

    private Uni<Void> guardarSismo(Sismo sismo) {
        if (sismo.magnitud < 1.0) {
            LOGGER.fine("⏭️ Sismo de magnitud muy baja: " + sismo.magnitud);
            return Uni.createFrom().voidItem();
        }

        return sismoYaExiste(sismo)
                .onItem().transformToUni(existe -> {
                    if (existe) {
                        LOGGER.fine("⏭️ Sismo duplicado: " + sismo.usgsId);
                        return Uni.createFrom().voidItem();
                    }

                    LOGGER.info("🌍 Nuevo sismo: " + sismo.pais + " - M" + sismo.magnitud + " (" + sismo.fuente + ")");

                    return Panache.withTransaction(sismo::persist)
                            .onItem().invoke(p -> {
                                eventBus.emitir(sismo);
                                LOGGER.info("📢 Sismo emitido al stream: " + sismo.lugar);
                            })
                            .onItem().transformToUni(p -> {
                                String mensaje = construirMensajeTelegram(sismo);
                                return telegramClient.enviarAlerta(botToken, chatId, mensaje, "HTML")
                                        .onItem().invoke(res -> LOGGER.info("📲 Alerta enviada a Telegram: " + sismo.pais))
                                        .onFailure().invoke(err -> LOGGER.warning("⚠️ Error Telegram: " + err.getMessage()))
                                        .onFailure().recoverWithItem(() -> null);
                            })
                            .onItem().transformToUni(result -> Uni.createFrom().voidItem());
                });
    }

    private String construirMensajeTelegram(Sismo sismo) {
        String fechaFormateada = sismo.fechaHora.format(dateFormatter);
        String emoji = sismo.alertaCritica ? "🔴🔴" : "🚨";
        String alerta = sismo.alertaCritica ? "¡ALERTA CRÍTICA!" : "ALERTA SÍSMICA";
        String bandera = obtenerBandera(sismo.pais);

        return emoji + " <b>" + alerta + "</b> " + emoji + "\n\n" +
                bandera + " <b>País:</b> " + sismo.pais + "\n" +
                "📊 <b>Magnitud:</b> " + String.format("%.1f", sismo.magnitud) + "\n" +
                "📍 <b>Ubicación:</b> " + sismo.lugar + "\n" +
                "📅 <b>Fecha/Hora:</b> " + fechaFormateada + "\n" +
                "🗺️ <b>Coordenadas:</b>\n" +
                "   • Latitud: " + String.format("%.4f", sismo.latitud) + "\n" +
                "   • Longitud: " + String.format("%.4f", sismo.longitud) + "\n" +
                "📏 <b>Profundidad:</b> " + String.format("%.1f", sismo.profundidad) + " km\n" +
                "🔗 <b>Fuente:</b> " + sismo.fuente + "\n\n" +
                "🔗 <a href='https://www.google.com/maps?q=" + sismo.latitud + "," + sismo.longitud + "'>Ver en Google Maps</a>";
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