package com.itsz.es.learning.client;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.minidev.json.annotate.JsonIgnore;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Person {

    @JsonIgnore
    private String id;

    private String name;

    private int age;

    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date birthday;
}
