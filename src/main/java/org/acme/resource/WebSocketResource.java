package org.acme.resource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

@ApplicationScoped
@ServerEndpoint("/api/sismos/ws")
public class WebSocketResource {

    private static final Logger LOGGER = Logger.getLogger(WebSocketResource.class.getName());

    // ✅ Cola de sesiones activas
    private final ConcurrentLinkedQueue<Session> sessions = new ConcurrentLinkedQueue<>();

    @OnOpen
    public void onOpen(Session session) {
        sessions.add(session);
        LOGGER.info("🔌 Cliente conectado vía WebSocket. Total: " + sessions.size());
    }

    @OnClose
    public void onClose(Session session) {
        sessions.remove(session);
        LOGGER.info("🔌 Cliente desconectado vía WebSocket. Total: " + sessions.size());
    }

    @OnMessage
    public String onMessage(String message) {
        LOGGER.info("📨 Mensaje recibido: " + message);
        return "Mensaje recibido: " + message;
    }

    // ✅ Método para enviar sismos a todos los clientes
    public void enviarSismo(String sismoJson) {
        for (Session session : sessions) {
            if (session.isOpen()) {
                try {
                    session.getBasicRemote().sendText(sismoJson);
                    LOGGER.info("📤 Sismo enviado vía WebSocket");
                } catch (IOException e) {
                    LOGGER.severe("❌ Error enviando sismo: " + e.getMessage());
                }
            }
        }
    }
}