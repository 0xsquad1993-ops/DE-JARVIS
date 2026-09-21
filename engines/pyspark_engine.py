from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    count,
    sum as spark_sum,
    avg,
)
from pyspark.sql.types import StringType


class PySparkEngine:

    def __init__(self):

        self.spark = (
            SparkSession.builder
            .appName("DE-JARVIS")
            .master("local[*]")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("ERROR")

        print()
        print("=" * 70)
        print("                 DE-JARVIS PYSPARK ENGINE")
        print("=" * 70)
        print(f"Spark Version : {self.spark.version}")
        print("Master        : local[*]")
        print("Status        : ONLINE")
        print("=" * 70)

    # ==========================================================
    # READ CSV
    # ==========================================================

    def read_csv(self, file_path):

        print()
        print("=" * 70)
        print("PYSPARK ENGINE: READING DATA")
        print("=" * 70)

        file_path = str(file_path)

        print(f"File: {file_path}")

        df = (
            self.spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("mode", "PERMISSIVE")
            .csv(file_path)
        )

        row_count = df.count()

        print(f"Rows loaded: {row_count}")

        print()
        print("SCHEMA")
        print("-" * 70)

        df.printSchema()

        return df

    # ==========================================================
    # DATA PROFILING
    # ==========================================================

    def profile(self, df):

        print()
        print("=" * 70)
        print("PYSPARK ENGINE: DATA PROFILING")
        print("=" * 70)

        row_count = df.count()
        column_count = len(df.columns)

        print(f"Rows    : {row_count}")
        print(f"Columns : {column_count}")

        print()
        print("COLUMNS")
        print("-" * 70)

        for column_name in df.columns:

            data_type = df.schema[column_name].dataType

            print(
                f"{column_name:<25}"
                f"{str(data_type):<20}"
            )

        print()
        print("NULL COUNTS")
        print("-" * 70)

        for column_name in df.columns:

            null_count = (
                df.filter(
                    col(column_name).isNull()
                )
                .count()
            )

            print(
                f"{column_name:<25}"
                f"{null_count}"
            )

        return {
            "rows": row_count,
            "columns": column_count,
            "column_names": df.columns,
        }

    # ==========================================================
    # CLEAN STRING COLUMNS
    # ==========================================================

    def clean_strings(self, df):

        print()
        print("PYSPARK ENGINE: Cleaning string columns...")

        for field in df.schema.fields:

            if isinstance(field.dataType, StringType):

                df = df.withColumn(
                    field.name,
                    trim(col(field.name))
                )

        print(
            "PYSPARK ENGINE: "
            "String cleanup completed"
        )

        return df

    # ==========================================================
    # REMOVE DUPLICATES
    # ==========================================================

    def remove_duplicates(self, df):

        print()
        print("PYSPARK ENGINE: Removing duplicates...")

        before_count = df.count()

        df = df.dropDuplicates()

        after_count = df.count()

        removed = before_count - after_count

        print(f"Rows before : {before_count}")
        print(f"Rows after  : {after_count}")
        print(f"Duplicates  : {removed}")

        return df

    # ==========================================================
    # REMOVE EMPTY ROWS
    # ==========================================================

    def remove_empty_rows(self, df):

        print()
        print("PYSPARK ENGINE: Checking empty rows...")

        before_count = df.count()

        df = df.dropna(
            how="all"
        )

        after_count = df.count()

        removed = before_count - after_count

        print(f"Rows before        : {before_count}")
        print(f"Empty rows removed : {removed}")

        return df

    # ==========================================================
    # TRANSFORMATION
    # ==========================================================

    def transform(self, df):

        print()
        print("=" * 70)
        print("PYSPARK ENGINE: TRANSFORMATION")
        print("=" * 70)

        df = self.remove_empty_rows(df)

        df = self.clean_strings(df)

        df = self.remove_duplicates(df)

        print()
        print("Transformation completed.")

        print()
        print("TRANSFORMED SCHEMA")
        print("-" * 70)

        df.printSchema()

        return df

    # ==========================================================
    # DATA QUALITY
    # ==========================================================

    def quality_check(self, df):

        print()
        print("=" * 70)
        print("PYSPARK ENGINE: DATA QUALITY")
        print("=" * 70)

        total_rows = df.count()

        total_nulls = 0

        for column_name in df.columns:

            null_count = (
                df.filter(
                    col(column_name).isNull()
                )
                .count()
            )

            total_nulls += null_count

        duplicate_count = (
            total_rows
            - df.dropDuplicates().count()
        )

        print(f"Total Rows : {total_rows}")
        print(f"Total NULLs: {total_nulls}")
        print(f"Duplicates : {duplicate_count}")

        if total_rows == 0:

            status = "FAIL"

        elif total_nulls > 0:

            status = "CHECK"

        elif duplicate_count > 0:

            status = "CHECK"

        else:

            status = "PASS"

        print()
        print(f"DATA QUALITY STATUS: {status}")

        return {
            "rows": total_rows,
            "nulls": total_nulls,
            "duplicates": duplicate_count,
            "status": status,
        }

    # ==========================================================
    # WRITE CSV
    # ==========================================================

    def write_csv(self, df, output_path):

        print()
        print("=" * 70)
        print("PYSPARK ENGINE: WRITING CSV")
        print("=" * 70)

        output_path = Path(output_path)

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        spark_output_path = (
            output_path.parent
            / f"{output_path.stem}_spark_output"
        )

        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(str(spark_output_path))
        )

        print(
            f"CSV OUTPUT: {spark_output_path}"
        )

        return True

    # ==========================================================
    # GOLD AGGREGATION
    # ==========================================================

    def aggregate(
        self,
        df,
        group_column,
        numeric_column
    ):

        print()
        print("=" * 70)
        print("PYSPARK ENGINE: GOLD AGGREGATION")
        print("=" * 70)

        if group_column not in df.columns:

            raise ValueError(
                f"Group column not found: {group_column}"
            )

        if numeric_column not in df.columns:

            raise ValueError(
                f"Numeric column not found: {numeric_column}"
            )

        result = (
            df.groupBy(group_column)
            .agg(
                count("*").alias("record_count"),

                spark_sum(
                    col(numeric_column)
                ).alias(
                    f"total_{numeric_column}"
                ),

                avg(
                    col(numeric_column)
                ).alias(
                    f"avg_{numeric_column}"
                ),
            )
        )

        print()
        print("GOLD RESULT")
        print("-" * 70)

        result.show(
            truncate=False
        )

        return result

    # ==========================================================
    # SHOW DATA
    # ==========================================================

    def show_data(self, df, rows=10):

        print()
        print("=" * 70)
        print("PYSPARK ENGINE: DATA PREVIEW")
        print("=" * 70)

        df.show(
            rows,
            truncate=False
        )

    # ==========================================================
    # STOP SPARK
    # ==========================================================

    def stop(self):

        print()
        print("=" * 70)
        print("PYSPARK ENGINE: SHUTTING DOWN")
        print("=" * 70)

        self.spark.stop()

        print("Status: OFFLINE")
        print("=" * 70)


# ==============================================================
# TEST
# ==============================================================

if __name__ == "__main__":

    engine = None

    try:

        engine = PySparkEngine()

        # ------------------------------------------------------
        # 1. READ
        # ------------------------------------------------------

        df = engine.read_csv(
            "data/raw/employees.csv"
        )

        # ------------------------------------------------------
        # 2. PROFILE
        # ------------------------------------------------------

        engine.profile(df)

        # ------------------------------------------------------
        # 3. TRANSFORMATION
        # ------------------------------------------------------

        transformed_df = engine.transform(
            df
        )

        # ------------------------------------------------------
        # 4. DATA QUALITY
        # ------------------------------------------------------

        quality = engine.quality_check(
            transformed_df
        )

        # ------------------------------------------------------
        # 5. PREVIEW
        # ------------------------------------------------------

        engine.show_data(
            transformed_df
        )

        # ------------------------------------------------------
        # 6. BRONZE
        # ------------------------------------------------------

        print()
        print("BRONZE LAYER")
        print("-" * 70)

        engine.write_csv(
            df,
            "data/bronze/employees"
        )

        # ------------------------------------------------------
        # 7. SILVER
        # ------------------------------------------------------

        print()
        print("SILVER LAYER")
        print("-" * 70)

        engine.write_csv(
            transformed_df,
            "data/silver/employees"
        )

        # ------------------------------------------------------
        # 8. GOLD
        # ------------------------------------------------------

        if (
            "city" in transformed_df.columns
            and
            "salary" in transformed_df.columns
        ):

            gold_df = engine.aggregate(
                transformed_df,
                "city",
                "salary"
            )

            print()
            print("GOLD LAYER")
            print("-" * 70)

            engine.write_csv(
                gold_df,
                "data/gold/employee_summary"
            )

        else:

            print()
            print(
                "GOLD: Required columns "
                "not found."
            )

        # ------------------------------------------------------
        # FINAL STATUS
        # ------------------------------------------------------

        print()
        print("=" * 70)
        print("             DE-JARVIS PYSPARK TEST")
        print("=" * 70)

        print(
            f"Rows Processed : {quality['rows']}"
        )

        print(
            f"NULLs          : {quality['nulls']}"
        )

        print(
            f"Duplicates     : {quality['duplicates']}"
        )

        print(
            f"Quality Status  : {quality['status']}"
        )

        print()
        print("FINAL STATUS: SUCCESS")
        print("=" * 70)

    except Exception as error:

        print()
        print("=" * 70)
        print("PYSPARK ENGINE ERROR")
        print("=" * 70)

        print(
            f"{type(error).__name__}: {error}"
        )

        print()
        print("FINAL STATUS: FAILED")

    finally:

        if engine is not None:

            engine.stop()