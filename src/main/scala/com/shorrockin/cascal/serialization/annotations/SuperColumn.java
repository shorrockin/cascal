package com.shorrockin.cascal.serialization.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * used to define that the attribute specified should be mapped to the
 * super column name. may only exist in family mappings of type super and
 * may only exist once per object.
 *
 * @author Chris Shorrock
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface SuperColumn {
}
