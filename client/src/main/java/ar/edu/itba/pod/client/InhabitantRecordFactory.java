package ar.edu.itba.pod.client;

import ar.edu.itba.pod.model.EmploymentCondition;
import ar.edu.itba.pod.model.InhabitantRecord;
import ar.edu.itba.pod.model.InhabitantRecordSerializationMode;
import ar.edu.itba.pod.model.Province;

public class InhabitantRecordFactory {
    public static InhabitantRecord create(EmploymentCondition condition, Integer homeId, String departmentName, Province province, InhabitantRecordSerializationMode mode) {
        if (!mode.isCondition()) {
            condition = null;
        }
        if (!mode.isHomeId()) {
            homeId = null;
        }
        if (!mode.isDepartmentName()) {
            departmentName = null;
        }
        if (!mode.isProvince()) {
            province = null;
        }
        return new InhabitantRecord(condition, homeId, departmentName, province, mode);
    };
}
