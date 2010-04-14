package com.shorrockin.cascal.serialization.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * annotation type which allows you to map all the columns returned into
 * a map of objects.
 *
 * @author Chris Shorrock
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Columns {
    Class<?> name();
    Class<?> value();
}
