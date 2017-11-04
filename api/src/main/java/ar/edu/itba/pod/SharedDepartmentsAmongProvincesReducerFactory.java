package ar.edu.itba.pod;

import com.hazelcast.core.ISet;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import javax.sound.midi.Soundbank;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
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
            /**
             * Todo: Make more efficient
             */
            for (Province p: s){
                for (Province o: s){
                    if(!p.equals(o))
                        ans.add(new ProvincePair(p,o));
                }
            }
            return ans;
        }
    }
}
