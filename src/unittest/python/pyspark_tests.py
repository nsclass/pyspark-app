"""
 Copyright (c) 2023 Nam Seob Seo
 
 This software is released under the MIT License.
 https://opensource.org/licenses/MIT
"""

import unittest
import os
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from nseo.app.service import hello

class HelloWorldTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        THIS_DIR = Path(__file__).parent
        cls.data_path = THIS_DIR.parent / 'data'
        cls.spark = SparkSession \
                    .builder \
                    .appName('pyspark-app') \
                    .getOrCreate()

        return None

    def test_should_run_pyspark(self):
        df = self.spark.read.csv(f"{self.data_path}/crash_catalonia.csv")
        row_count = df.count()
        print(f"Row count: {row_count}")
        hello()

    def test_custom_agg(self):
        df = self.spark \
            .read \
            .option("header", "true") \
            .option("inferschema", "true") \
            .csv(f"{self.data_path}/price.csv")
        df.printSchema()

        cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
        df.groupBy(df.date).agg(F.avg(df.price).alias('avg'),
                                cnt_cond(df.include == 'true').alias('count_cnd')) \
                                .show()