package org.acme.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;  // ✅ AGREGAR IMPORT
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import org.acme.model.Sismo;
import org.acme.resource.WebSocketResource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import java.util.logging.Logger;

@ApplicationScoped
public class SismoEventBus {

    private static final Logger LOGGER = Logger.getLogger(SismoEventBus.class.getName());
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());  // ✅ REGISTRAR EL MÓDULO

    private final BroadcastProcessor<Sismo> processor = BroadcastProcessor.create();

    @Inject
    WebSocketResource webSocketResource;

    @PostConstruct
    public void init() {
        LOGGER.info("🔌 Iniciando SismoEventBus con suscriptor permanente...");
        processor.subscribe().with(
                sismo -> {
                    try {
                        String sismoJson = objectMapper.writeValueAsString(sismo);
                        webSocketResource.enviarSismo(sismoJson);
                    } catch (Exception e) {
                        LOGGER.severe("❌ Error serializando sismo: " + e.getMessage());
                    }
                },
                failure -> LOGGER.severe("❌ Error en el suscriptor permanente: " + failure.getMessage())
        );
        LOGGER.info("✅ SismoEventBus iniciado correctamente");
    }

    public void emitir(Sismo sismo) {
        LOGGER.info("📤 Emitiendo sismo: " + sismo.lugar + " (M" + sismo.magnitud + ")");
        try {
            processor.onNext(sismo);
            LOGGER.info("✅ Sismo emitido correctamente");
        } catch (Exception e) {
            LOGGER.severe("❌ Error al emitir sismo: " + e.getMessage());
        }
    }

    public BroadcastProcessor<Sismo> getProcessor() {
        LOGGER.info("📡 Cliente solicitando stream SSE");
        return processor;
    }

    public Multi<Sismo> getStream() {
        LOGGER.info("📡 Cliente solicitando stream SSE (via getStream)");
        return processor;
    }
}
