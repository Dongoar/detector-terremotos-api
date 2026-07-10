package org.acme.service;

import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import org.acme.model.Sismo;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class SismoEventBus {
    // Este procesador permite que múltiples clientes se "suscriban" a los eventos
    private final BroadcastProcessor<Sismo> processor = BroadcastProcessor.create();

    public void emitir(Sismo sismo) {
        processor.onNext(sismo);
    }

    public BroadcastProcessor<Sismo> getProcessor() {
        return processor;
    }
}