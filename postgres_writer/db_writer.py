def write_aggregates_to_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/analytics") \
        .option("dbtable", "event_counts") \
        .option("user", "postgres") \
        .option("password", "Predator@123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


def write_funnel_to_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/analytics") \
        .option("dbtable", "funnel_summary") \
        .option("user", "postgres") \
        .option("password", "Predator@123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


def write_trends_to_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/analytics") \
        .option("dbtable", "event_trends") \
        .option("user", "postgres") \
        .option("password", "Predator@123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
