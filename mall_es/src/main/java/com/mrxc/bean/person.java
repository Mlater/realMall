package com.mrxc.bean;

import java.util.Objects;

public class person {
    private Integer id;
    private String name;
    private String sex;
    private String favo;

    public person() {
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getFavo() {
        return favo;
    }

    public void setFavo(String favo) {
        this.favo = favo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        person person = (person) o;
        return Objects.equals(id, person.id) &&
                Objects.equals(name, person.name) &&
                Objects.equals(sex, person.sex) &&
                Objects.equals(favo, person.favo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, sex, favo);
    }

    @Override
    public String toString() {
        return "person{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", favo='" + favo + '\'' +
                '}';
    }
}
