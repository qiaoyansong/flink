package bean;

import java.util.Objects;

/**
 * @author ：Yunchenyan
 * @date ：Created in 2024/11/24 11:19
 * description：
 */
public class WaterSensor {

    /**
     * 传感器ID
     */
    private String id;

    /**
     * 时间戳
     */
    private Long ts;

    /**
     * 水位
     */
    private Integer vc;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensor that = (WaterSensor) o;
        return getId().equals(that.getId()) && getTs().equals(that.getTs()) && getVc().equals(that.getVc());
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getTs(), getVc());
    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public WaterSensor() {
    }
}
