import swellow

swellow.peck(
    db="sc://localhost:15002",
    directory="./examples/databricks-delta/migrations",
    engine="spark-delta",
    json=True
)
