package com.mrxc.mallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;


@Data
@AllArgsConstructor
public class stat {
    private List<Option> options;
    private String title;
}
