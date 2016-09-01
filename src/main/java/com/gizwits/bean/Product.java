package com.gizwits.bean;

/**
 * Created by feel on 16/6/2.
 */
public class Product {


    private String timetamp;

    private String product_key;

    private String mac;

    private Long count;

    public Product() {
    }

    public Product(String timetamp, String product_key, String mac, Long count) {
        this.timetamp = timetamp;
        this.product_key = product_key;
        this.mac = mac;
        this.count = count;
    }

    public String getTimetamp() {
        return timetamp;
    }

    public void setTimetamp(String timetamp) {
        this.timetamp = timetamp;
    }

    public String getProduct_key() {
        return product_key;
    }

    public void setProduct_key(String product_key) {
        this.product_key = product_key;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "Product{" +
                "timetamp='" + timetamp + '\'' +
                ", product_key='" + product_key + '\'' +
                ", mac='" + mac + '\'' +
                ", count=" + count +
                '}';
    }
}
