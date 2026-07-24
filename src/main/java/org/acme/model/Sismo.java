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

    // ✅ NUEVO: Profundidad en kilómetros
    public double profundidad = 0;

    @Column(nullable = false)
    public boolean alertaCritica = false;

    @Column(nullable = false)
    public String usuarioId;

    // ✅ NUEVO: País del sismo (Perú, Chile, Argentina, etc.)
    @Column(nullable = false)
    public String pais = "Desconocido";

    // ✅ NUEVO: Fuente del dato (IGP, USGS, CSN, etc.)
    @Column(nullable = false)
    public String fuente = "USGS";

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
                ", profundidad=" + profundidad +
                ", alertaCritica=" + alertaCritica +
                ", usuarioId='" + usuarioId + '\'' +
                ", pais='" + pais + '\'' +
                ", fuente='" + fuente + '\'' +
                '}';
    }
}