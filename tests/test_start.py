import os
from datetime import datetime

import pytest
from pyspark import Row

from solution.solution_start import get_latest_transaction_date, run_transformations, to_canonical_date_str, \
    create_spark_views


@pytest.mark.usefixtures("spark")
def test_get_latest_transaction_date_returns_most_recent_date(spark):
    spark.createDataFrame([
        Row(date_of_purchase=datetime(2018, 12, 1, 4, 15, 0)),
        Row(date_of_purchase=datetime(2019, 3, 1, 14, 10, 0)),
        Row(date_of_purchase=datetime(2019, 2, 1, 14, 9, 59)),
        Row(date_of_purchase=datetime(2019, 1, 2, 19, 14, 20))
    ]).createOrReplaceTempView("raw_transactions")

    expected = datetime(2019, 3, 1, 14, 10, 0)
    actual = get_latest_transaction_date(spark)

    assert actual == expected


@pytest.mark.usefixtures("spark")
def test_run_transformations(spark):
    root_path = os.getcwd()
    customers_path = root_path + "/test_data/input_data/customers.csv"
    products_path = root_path + "/test_data/input_data/products.csv"
    transactions_path = root_path + "/test_data/input_data/transactions"
    output_path = root_path + "/test_data/output_data/customer_purchase"
    expected_output_path = root_path + "/test_data/output_data_expected/customer_purchase"
    expected = spark.read.json(expected_output_path)
    run_transformations(spark, customers_location=customers_path, products_location=products_path,
                        transactions_location=transactions_path, output_location=output_path)
    actual = spark.read.json(output_path)
    assert expected.subtract(actual).count() == 0


def test_to_canonical_date_str():
    date = datetime(2022, 6, 23)
    actual = to_canonical_date_str(date)
    expected = "2022-06-23"
    assert actual == expected


@pytest.mark.usefixtures("spark")
def test_create_spark_views_creation(spark):
    root_path = os.getcwd()
    customers_path = root_path + "/test_data/input_data/customers.csv"
    products_path = root_path + "/test_data/input_data/products.csv"
    transactions_path = root_path + "/test_data/input_data/transactions"
    create_spark_views(spark=spark, customers_location=customers_path, products_location=products_path,
                       transactions_location=transactions_path)
    actual_tables = set([table.name for table in spark.catalog.listTables()])
    expected_tables = {'customers', 'products', 'raw_transactions'}
    assert expected_tables.issubset(actual_tables)


@pytest.mark.usefixtures("spark")
def test_create_spark_views_data_load(spark):
    root_path = os.getcwd()
    customers_path = root_path + "/test_data/input_data/customers.csv"
    products_path = root_path + "/test_data/input_data/products.csv"
    transactions_path = root_path + "/test_data/input_data/transactions"
    create_spark_views(spark=spark, customers_location=customers_path, products_location=products_path,
                       transactions_location=transactions_path)
    assert spark.sql("select count(*) from customers").count() > 0 and spark.sql(
        "select count(*) from products").count() > 0 and spark.sql("select count(*) from raw_transactions").count() > 0
