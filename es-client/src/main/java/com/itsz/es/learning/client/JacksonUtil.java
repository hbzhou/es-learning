package com.itsz.es.learning.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonUtil {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static <T> String object2String(T object) throws JsonProcessingException {
        return objectMapper.writeValueAsString(object);
    }
}
