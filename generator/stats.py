"""`stats`: a quick look at what's in the source database."""

QUERIES = {
    "Row counts": """
        SELECT 'merchants (active)', count(*) FILTER (WHERE status = 'active') || ' / ' || count(*)
          FROM merchants
        UNION ALL SELECT 'customers', count(*)::text FROM customers
        UNION ALL SELECT 'payment methods (active)',
                         count(*) FILTER (WHERE is_active) || ' / ' || count(*) FROM payment_methods
        UNION ALL SELECT 'payments', count(*)::text FROM payments
        UNION ALL SELECT 'refunds', count(*)::text FROM refunds""",
    "Payments by status (count, INR)": """
        SELECT status, count(*), to_char(sum(amount_paise) / 100.0, 'FM99,99,99,99,999')
        FROM payments GROUP BY status ORDER BY count(*) DESC""",
    "Attempt success rate by method (count, success %)": """
        SELECT pm.method_type, count(*),
               round(100.0 * avg((p.status = 'captured')::int), 1)
        FROM payments p JOIN payment_methods pm USING (payment_method_id)
        WHERE p.status IN ('captured', 'failed')
        GROUP BY pm.method_type ORDER BY count(*) DESC""",
    "Orders (count, converted %, avg attempts)": """
        SELECT count(*), round(100.0 * avg(converted::int), 1), round(avg(attempts), 2)
        FROM (SELECT bool_or(status = 'captured') AS converted, count(*) AS attempts
              FROM payments GROUP BY merchant_id, merchant_order_id) o""",
    "Refunds by status (count, INR)": """
        SELECT status, count(*), to_char(sum(amount_paise) / 100.0, 'FM99,99,99,99,999')
        FROM refunds GROUP BY status ORDER BY count(*) DESC""",
    "Rows changed in the last 60 s": """
        SELECT 'payments', count(*) FROM payments WHERE updated_at > now() - interval '60 s'
        UNION ALL SELECT 'refunds', count(*) FROM refunds WHERE updated_at > now() - interval '60 s'
        UNION ALL SELECT 'merchants', count(*) FROM merchants WHERE updated_at > now() - interval '60 s'
        UNION ALL SELECT 'customers', count(*) FROM customers WHERE updated_at > now() - interval '60 s'""",
}


def print_stats(conn):
    for title, query in QUERIES.items():
        print(f"\n{title}")
        for row in conn.execute(query):
            print("  " + "  ".join(f"{v!s:<26}" if i == 0 else f"{v!s:>12}"
                                    for i, v in enumerate(row)).rstrip())
