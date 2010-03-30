package com.shorrockin.cascal

/**
 * used for when you don't want any predicate
 */
case object EmptyPredicate extends RangePredicate(None, None, Order.Ascending, None)