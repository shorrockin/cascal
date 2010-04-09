package com.shorrockin.cascal.serialization.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * marked an an attribute that we wish to used as a column value. currently
 * limited to columns where the name value is encoded as a utf-8 string.
 *
 * @author Chris Shorrock
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Value {
    // TOOD would like to use a byte[] here but it needs to be a constant.
    String value();
}
