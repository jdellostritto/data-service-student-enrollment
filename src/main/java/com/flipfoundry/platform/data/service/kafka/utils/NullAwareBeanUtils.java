package com.flipfoundry.platform.data.service.kafka.utils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtilsBean;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 *
 * Copyright 2024-2026 Flipfoundry.<br><br>
 *
 * Project:    booking-data-service<br>
 * Class:      NullAwareBeanUtils.java<br>
 * References: `NA`<br><br>
 *
 * @implSpec `Overriden copy properties so we can capture the type and set a non-null value.`<br>
 *
 *
 * @author Flipfoundry Team
 * @since 2021-08-15
 * @implNote Modernized for Java 21 and Spring Boot 3.5.8 in 2026
 *
 *
 */
@Slf4j
public class NullAwareBeanUtils extends BeanUtilsBean {

    /**
     * +-----------+---------+-------------------------+
     * | Data Type | Size    | Range                   |
     * +-----------+---------+-------------------------+
     * | byte      | 1 byte  | -128 to 127             |
     * | short     | 2 bytes | -32,768 to 32,767       |
     * | int       | 4 bytes | -2^31 to 2^31-1         |
     * | long      | 8 bytes | -2^63 to 2^63-1         |
     * | float     | 4 bytes | -3.4e38 to 3.4e38       |
     * | double    | 8 bytes | -1.7e308 to 1.7e308     |
     * | boolean   | 1 bit*  | true or false           |
     * | char      | 2 bytes | '\u0000' to '\uffff'    |
     * +-----------+---------+-------------------------+
     * @param dest
     * @param name
     * @param value
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @SneakyThrows
    @Override
    public void copyProperty(Object dest, String name, Object value)
            throws IllegalAccessException, InvocationTargetException {
        if(value==null){
            Class<?> c = dest.getClass();
            Field f = c.getDeclaredField(name);
            f.setAccessible(true);

            if (f.getType().isAssignableFrom(String.class) )
                super.copyProperty(dest, name, "UNSET");
            else if (f.getType().isAssignableFrom(long.class))
                super.copyProperty(dest, name, -1);
            else if (f.getType().isAssignableFrom(int.class))
                super.copyProperty(dest, name, -1);
            else if (f.getType().isAssignableFrom(double.class))
                super.copyProperty(dest, name, -1);
            else
                log.error("NULL VALUE TYPE NOT HANDLED!");
        }
        else
            super.copyProperty(dest, name, value);
    }


}
