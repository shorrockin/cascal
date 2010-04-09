package com.shorrockin.cascal.serialization.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * attaches to a single value in a class with a family mapping annotation to
 * indicate that the value specified should be used as the key for this
 * entry. Map only exist on a single attribute.
 *
 * @author Chris Shorrock
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Key {}
