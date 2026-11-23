from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum

PARQUET_PATH = "/opt/spark-apps/storage/parquet/telemetry"


def main():
    spark = (
        SparkSession.builder
        .appName("CarbonHive-Anomaly-Evaluation")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    print("\n========== CARBONHIVE DETECTION EVALUATION ==========")

    df = spark.read.parquet(PARQUET_PATH)

    required_columns = {
        "ground_truth_anomaly",
        "detected_anomaly",
    }

    missing = required_columns - set(df.columns)

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    evaluated = df.filter(
        col("ground_truth_anomaly").isNotNull()
        & col("detected_anomaly").isNotNull()
    )

    counts = evaluated.agg(
        spark_sum(
            when(
                col("ground_truth_anomaly") & col("detected_anomaly"),
                1
            ).otherwise(0)
        ).alias("TP"),

        spark_sum(
            when(
                (~col("ground_truth_anomaly")) & col("detected_anomaly"),
                1
            ).otherwise(0)
        ).alias("FP"),

        spark_sum(
            when(
                (~col("ground_truth_anomaly")) & (~col("detected_anomaly")),
                1
            ).otherwise(0)
        ).alias("TN"),

        spark_sum(
            when(
                col("ground_truth_anomaly") & (~col("detected_anomaly")),
                1
            ).otherwise(0)
        ).alias("FN")
    ).first()

    tp = counts["TP"] or 0
    fp = counts["FP"] or 0
    tn = counts["TN"] or 0
    fn = counts["FN"] or 0

    total = tp + fp + tn + fn

    accuracy = (tp + tn) / total if total else 0
    precision = tp / (tp + fp) if (tp + fp) else 0
    recall = tp / (tp + fn) if (tp + fn) else 0

    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall)
        else 0
    )

    print("\n========== CONFUSION MATRIX ==========")
    print(f"True Positives  (TP): {tp}")
    print(f"False Positives (FP): {fp}")
    print(f"True Negatives  (TN): {tn}")
    print(f"False Negatives (FN): {fn}")
    print(f"Total evaluated records: {total}")

    print("\n========== EVALUATION METRICS ==========")
    print(f"Accuracy : {accuracy:.4f} ({accuracy * 100:.2f}%)")
    print(f"Precision: {precision:.4f} ({precision * 100:.2f}%)")
    print(f"Recall   : {recall:.4f} ({recall * 100:.2f}%)")
    print(f"F1-score : {f1:.4f} ({f1 * 100:.2f}%)")

    print("\n========== EVALUATION COMPLETED ==========")

    spark.stop()


if __name__ == "__main__":
    main()
