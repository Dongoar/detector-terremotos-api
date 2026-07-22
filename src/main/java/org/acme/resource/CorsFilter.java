package org.acme.resource;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;

@Provider
public class CorsFilter implements ContainerResponseFilter {

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        // ✅ Agregar CORS solo si no existe
        if (!responseContext.getHeaders().containsKey("Access-Control-Allow-Origin")) {
            responseContext.getHeaders().add("Access-Control-Allow-Origin", "*");
        }
        if (!responseContext.getHeaders().containsKey("Access-Control-Allow-Methods")) {
            responseContext.getHeaders().add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD");
        }
        if (!responseContext.getHeaders().containsKey("Access-Control-Allow-Headers")) {
            responseContext.getHeaders().add("Access-Control-Allow-Headers", "accept, authorization, content-type, x-requested-with, origin, cache-control, ngrok-skip-browser-warning");
        }
        if (!responseContext.getHeaders().containsKey("Access-Control-Allow-Credentials")) {
            responseContext.getHeaders().add("Access-Control-Allow-Credentials", "true");
        }
        if (!responseContext.getHeaders().containsKey("Access-Control-Max-Age")) {
            responseContext.getHeaders().add("Access-Control-Max-Age", "86400");
        }
    }
}
