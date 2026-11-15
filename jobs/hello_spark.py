from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Abre a sessão
spark = (
    SparkSession.builder
    .appName("hello_spark")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

# 2. Cria um DataFrame pequeno
data = [
    ("credit_card", 10.0),
    ("cash", 5.0),
    ("credit_card", 20.0),
    ("cash", 7.5),
    ("dispute", 3.0),
]
df = spark.createDataFrame(data, ["payment_type", "amount"])

# 3. Transformation: ainda não executa nada
result = df.groupBy("payment_type").agg(
    F.count("*").alias("trips"),
    F.sum("amount").alias("total"),
)

# 4. Plano (sem executar)
result.explain()

# 5. Action: aqui o Spark executa de verdade
result.show()

spark.stop()