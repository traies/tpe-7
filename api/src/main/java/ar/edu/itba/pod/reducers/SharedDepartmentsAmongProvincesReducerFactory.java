package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.model.Province;
import ar.edu.itba.pod.model.ProvincePair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class SharedDepartmentsAmongProvincesReducerFactory implements ReducerFactory<String, Set<Province>, Set<ProvincePair>> {

    @Override
    public Reducer<Set<Province>, Set<ProvincePair>> newReducer(String s ) {
        return new SharedDepartmentsAmongProvincesReducer();
    }

    private class SharedDepartmentsAmongProvincesReducer extends Reducer<Set<Province>, Set<ProvincePair>> {

        private Set<Province> s = new HashSet<>();

        @Override
        public void reduce(Set<Province> value) {
            s.addAll(value);
        }

        @Override
        public Set<ProvincePair> finalizeReduce() {
            Set<ProvincePair> ans = new HashSet<>();
            Iterator<Province> provinceIterator = s.iterator();
            while(provinceIterator.hasNext()){
                Province p = provinceIterator.next();
                provinceIterator.remove();
                for(Province o : s)
                    ans.add(new ProvincePair(p,o));
            }

            return ans;
        }
    }
}
