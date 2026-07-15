package org.acme.model;

import io.quarkus.hibernate.reactive.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import java.time.LocalDateTime;

@Entity
@Table(name = "sismos_detectados")
public class Sismo extends PanacheEntity {

    @Column(unique = true, nullable = false)
    public String usgsId;

    public double magnitud;
    public String lugar;
    public LocalDateTime fechaHora;
    public double latitud;
    public double longitud;

    @Column(nullable = false)
    public boolean alertaCritica = false;

    // ✅ NUEVO: ID del usuario que guardó el sismo
    @Column(nullable = false)
    public String usuarioId;

    // ✅ Agregar toString() para logs
    @Override
    public String toString() {
        return "Sismo{" +
                "id=" + id +
                ", usgsId='" + usgsId + '\'' +
                ", magnitud=" + magnitud +
                ", lugar='" + lugar + '\'' +
                ", fechaHora=" + fechaHora +
                ", latitud=" + latitud +
                ", longitud=" + longitud +
                ", alertaCritica=" + alertaCritica +
                ", usuarioId='" + usuarioId + '\'' +
                '}';
    }
}