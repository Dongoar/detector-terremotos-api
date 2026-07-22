package org.acme.service;

import io.quarkus.websockets.next.OnTextMessage;
import io.quarkus.websockets.next.WebSocketClient;
import io.quarkus.websockets.next.WebSocketClientConnection;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.logging.Logger;

@WebSocketClient(path = "/graphql")
@ApplicationScoped
public class IgpWebSocketClient {

    private static final Logger LOGGER = Logger.getLogger(IgpWebSocketClient.class.getName());

    @Inject
    EarthquakeMonitor earthquakeMonitor;

    @OnTextMessage
    public void onMessage(String message, WebSocketClientConnection connection) {
        LOGGER.info("📩 Mensaje recibido del WebSocket IGP");
        LOGGER.info("📄 Contenido: " + message.substring(0, Math.min(300, message.length())));
        earthquakeMonitor.procesarMensajeWebSocket(message);
    }

    /**
     * Envía el mensaje connection_init
     */
    public void sendConnectionInit(WebSocketClientConnection connection) {
        String initMessage = """
            {
                "type": "connection_init",
                "payload": {}
            }
        """;
        connection.sendText(initMessage);
        LOGGER.info("📤 'connection_init' enviado");
    }

    /**
     * Envía la suscripción para newEarthquake
     */
    public void sendSubscriptionNewEarthquake(WebSocketClientConnection connection) {
        String subscribeMessage = """
            {
                "type": "subscribe",
                "id": "1",
                "payload": {
                    "query": "subscription { newEarthquake { id magnitud lugar latitud longitud profundidad fechaHora } }"
                }
            }
        """;
        connection.sendText(subscribeMessage);
        LOGGER.info("📤 Suscripción 'newEarthquake' enviada");
    }

    /**
     * Envía la suscripción para sismo
     */
    public void sendSubscriptionSismo(WebSocketClientConnection connection) {
        String subscribeMessage = """
            {
                "type": "subscribe",
                "id": "2",
                "payload": {
                    "query": "subscription { sismo { id magnitud referencia latitud longitud profundidad fecha_hora } }"
                }
            }
        """;
        connection.sendText(subscribeMessage);
        LOGGER.info("📤 Suscripción 'sismo' enviada");
    }
}