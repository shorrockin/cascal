package com.shorrockin.cascal.serialization.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * when this annotation is attached to a class it indicates that this class
 * is mapped to the specified column family.
 *
 * @author Chris Shorrock
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Family {
    String value();
}
