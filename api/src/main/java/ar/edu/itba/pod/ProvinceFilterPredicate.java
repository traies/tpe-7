package ar.edu.itba.pod;

import com.hazelcast.mapreduce.KeyPredicate;

public class ProvinceFilterPredicate implements KeyPredicate<Province> {
    private Province province;
    public ProvinceFilterPredicate(Province province) {
        this.province = province;
    }

    @Override
    public boolean evaluate(Province province) {
        return this.province.equals(province);
    }
}
