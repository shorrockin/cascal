package com.shorrockin.cascal.serialization.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * an optional type of value that is used when a column may or may
 * not exist. Uses scala Option/None concepts.
 *
 * @author Chris Shorrock
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Optional {
    String column();
    Class<?> as();
}
