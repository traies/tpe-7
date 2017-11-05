package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class SharedDepartmentsAmongProvincesReducerFactory implements ReducerFactory<String, Province, Set<ProvincePair>> {

    @Override
    public Reducer<Province, Set<ProvincePair>> newReducer(String s ) {
        return new EchoReducer();
    }

    private class EchoReducer extends Reducer<Province, Set<ProvincePair>> {

        private Set<Province> s = new HashSet<>();

        @Override
        public void reduce(Province value) {
            s.add(value);
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
