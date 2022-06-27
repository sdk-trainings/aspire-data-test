import argparse

from pyspark.sql import SparkSession


def create_spark_views(spark: SparkSession, customers_location: str, products_location: str,
                       transactions_location: str):
    spark.read.csv(customers_location, header=True).createOrReplaceTempView("customers")
    spark.read.csv(products_location, header=True).createOrReplaceTempView("products")
    spark.read.json(transactions_location).createOrReplaceTempView("raw_transactions")


def run_transformations(spark: SparkSession, customers_location: str, products_location: str,
                        transactions_location: str, output_location: str):
    create_spark_views(spark, customers_location, products_location, transactions_location)
    sql_query = """
    select tran.customer_id, c.loyalty_score, tran.product_id, p.product_category, tran.purchase_count from
    (select customer_id, bkt.product_id product_id, count(*) purchase_count
    from raw_transactions lateral view explode(basket) as bkt group by customer_id, bkt.product_id ) tran
    join  customers c on c.customer_id = tran.customer_id
    join products p on tran.product_id = p.product_id
                """

    spark.sql(sql_query).write.mode('overwrite').json(output_location)


def get_latest_transaction_date(spark: SparkSession):
    result = spark.sql("""SELECT MAX(date_of_purchase) AS date_of_purchase FROM raw_transactions""").collect()[0]
    max_date = result.date_of_purchase
    return max_date


def to_canonical_date_str(date_to_transform):
    return date_to_transform.strftime('%Y-%m-%d')


if __name__ == "__main__":
    spark_session = (
            SparkSession.builder
                        .master("local[2]")
                        .appName("DataTest")
                        .config("spark.executorEnv.PYTHONHASHSEED", "0")
                        .getOrCreate()
    )

    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data_expected/customer_purchase/")
    args = vars(parser.parse_args())

    run_transformations(spark_session, args['customers_location'], args['products_location'],
                        args['transactions_location'], args['output_location'])
