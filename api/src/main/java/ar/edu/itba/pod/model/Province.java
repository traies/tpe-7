package ar.edu.itba.pod.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by traies on 27/10/17.
 */
public enum Province {
    JUJUY("Jujuy",Region.REGION_DEL_NORTE_GRANDE_ARGENTINO),
    SALTA("Salta",Region.REGION_DEL_NORTE_GRANDE_ARGENTINO),
    FORMOSA("Formosa",Region.REGION_DEL_NORTE_GRANDE_ARGENTINO),
    CHACO("Chaco",Region.REGION_DEL_NORTE_GRANDE_ARGENTINO),
    CORRIENTES("Corrientes",Region.REGION_DEL_NORTE_GRANDE_ARGENTINO),
    MISIONES("Misiones",Region.REGION_DEL_NORTE_GRANDE_ARGENTINO),
    CATAMARCA("Catamarca",Region.REGION_DEL_NORTE_GRANDE_ARGENTINO),
    TUCUMAN("Tucumán",Region.REGION_DEL_NORTE_GRANDE_ARGENTINO),
    SANTIAGO_DEL_ESTERO("Santiago del Estero", Region.REGION_DEL_NORTE_GRANDE_ARGENTINO),
    SANTA_FE("Santa Fe",Region.REGION_CENTRO),
    ENTRE_RIOS("Entre Ríos",Region.REGION_CENTRO),
    LA_RIOJA("La Rioja",Region.REGION_DEL_NUEVO_CUYO),
    CORDOBA("Córdoba",Region.REGION_CENTRO),
    BUENOS_AIRES("Buenos Aires",Region.REGION_BUENOS_AIRES),
    CIUDAD_AUTONOMA_DE_BUENOS_AIRES("Ciudad Autónoma de Buenos Aires",Region.REGION_BUENOS_AIRES),
    SAN_JUAN("San Juan",Region.REGION_DEL_NUEVO_CUYO),
    SAN_LUIS("San Luis",Region.REGION_DEL_NUEVO_CUYO),
    MENDOZA("Mendoza",Region.REGION_DEL_NUEVO_CUYO),
    LA_PAMPA("La Pampa",Region.REGION_PATAGONICA),
    NEUQUEN("Neuquén",Region.REGION_PATAGONICA),
    RIO_NEGRO("Río negro",Region.REGION_PATAGONICA),
    CHUBUT("Chubut",Region.REGION_PATAGONICA),
    SANTA_CRUZ("Santa Cruz",Region.REGION_PATAGONICA),
    TIERRA_DEL_FUEGO("Tierra del Fuego",Region.REGION_PATAGONICA);

    private static Map<String, Province> map = new HashMap<>();
    private static Map<Integer, Province> ordinalMap = new HashMap<>();

    private String name;
    private Region region;

    public Region getRegion() {
        return region;
    }

    static {
        for(Province p:Province.values()) {
            map.put(p.name,p);
            ordinalMap.put(p.ordinal(), p);
        }
    }

    Province(String name,Region region){
        this.name = name;
        this.region = region;
    }

    public String getName(){
        return name;
    }

    public static Province getProvince(String s){
        return Optional.ofNullable(map.get(s)).orElseThrow(() -> new IllegalArgumentException(String.format("Not a province: %s", s)));
    }

    public static Province getProvinceByOrdinal(Integer ordinal){
        return Optional.ofNullable(ordinalMap.get(ordinal)).orElseThrow(() -> new IllegalArgumentException(String.format("Not a province: %d", ordinal)));
    }

    @Override
    public String toString() {
        return getName();
    }
}
