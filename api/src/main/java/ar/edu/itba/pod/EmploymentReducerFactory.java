package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

/**
 * El EmploymentReducer lleva la cuenta de cuantos habitantes con condicion Empleado y Desempleado hay. Si el registro
 * esta en condicion INACTIVO o DESCONOCIDO, se los ignora. Para finalizar, se divide la cantidad de Desempleados con
 * la suma de Empleados y Desempleados, tal como dice la formula en la consigna.
 * Para el caso en que la suma de Empleados y Desempleados sea cero, decidimos que el resultado sea tambien cero, porque
 * es el valor que mejor se adecua a lo que quiero representar (si no hay poblacion activa, la tasa de desempleo es cero)
 *
 * @Author tomas raies
 */
public class EmploymentReducerFactory implements ReducerFactory<Region, InhabitantRecord, Double> {
    @Override
    public Reducer<InhabitantRecord, Double> newReducer(Region s) {
        return new EmploymentReducerFactory.EmploymentReducer();
    }

    private class EmploymentReducer extends Reducer<InhabitantRecord, Double> {

        private Long employedPerRegion = 0L;
        private Long unemployedPerRegion = 0L;

        @Override
        public void reduce(InhabitantRecord value) {
            if (EmploymentCondition.EMPLOYED.equals(value.getCondition())) {
                employedPerRegion++;
            } else if (EmploymentCondition.UNEMPLOYED.equals(value.getCondition())) {
                unemployedPerRegion++;
            }
        }

        @Override
        public Double finalizeReduce() {
            if (employedPerRegion + unemployedPerRegion == 0) {
                return 0.0;
            }
            return unemployedPerRegion.doubleValue() / (unemployedPerRegion.doubleValue() + employedPerRegion.doubleValue());
        }
    }

}
