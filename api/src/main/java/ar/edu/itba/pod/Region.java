package ar.edu.itba.pod;

/**
 * Created by traies on 27/10/17.
 */
public enum Region {
    REGION_BUENOS_AIRES("Región Buenos Aires"),
    REGION_CENTRO("Región Centro"),
    REGION_DEL_NORTE_GRANDE_ARGENTINO("Región Del Norte Grande Argentino"),
    REGION_DEL_NUEVO_CUYO("Región Del Nuevo Cuyo"),
    REGION_PATAGONICA("Región Patagónica");

    private String name;

    Region(String name){
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
