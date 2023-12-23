"""
 Copyright (c) 2023 Nam Seob Seo
 
 This software is released under the MIT License.
 https://opensource.org/licenses/MIT
"""

import unittest
import os
from pathlib import Path
from pyspark.sql import SparkSession
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

    def test_run(self):
        hello()