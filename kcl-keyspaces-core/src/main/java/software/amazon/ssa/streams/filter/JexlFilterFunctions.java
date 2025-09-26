package software.amazon.ssa.streams.filter;

public final class JexlFilterFunctions {
    public static java.math.BigDecimal to_big_decimal(Object v) {
        if (v == null) return null;
        if (v instanceof Double) return new java.math.BigDecimal((Double) v);
        if (v instanceof Float) return new java.math.BigDecimal((Float) v);
        if (v instanceof Integer) return new java.math.BigDecimal((Integer) v);
        if (v instanceof Long) return new java.math.BigDecimal((Long) v);
        if (v instanceof Short) return new java.math.BigDecimal((Short) v);
        if (v instanceof Byte) return new java.math.BigDecimal((Byte) v);
        if (v instanceof String) return new java.math.BigDecimal((String) v);
        if (v instanceof java.math.BigDecimal) return (java.math.BigDecimal) v;
        if (v instanceof Number) return new java.math.BigDecimal(v.toString());
        if (v instanceof CharSequence) return new java.math.BigDecimal(v.toString());

        throw new IllegalArgumentException("to_big_decimal(): unsupported object type: " + v.getClass().getSimpleName());
    }
    public static int compare_to(java.math.BigDecimal a, Object b) {
        return a.compareTo(to_big_decimal(b));
    }
}
