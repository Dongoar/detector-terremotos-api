package org.acme.model;

import io.quarkus.hibernate.reactive.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import java.time.LocalDateTime;

@Entity
@Table(name = "sismos_detectados")
public class Sismo extends PanacheEntity {

    @Column(unique = true) // Esto asegura a nivel de BD que no haya duplicados
    public String usgsId;  // <-- ID único de la API

    public double magnitud;
    public String lugar;
    public LocalDateTime fechaHora;
    public double latitud;
    public double longitud;
    public boolean alertaCritica;
}