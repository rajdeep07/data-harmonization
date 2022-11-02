from typing import Optional
from splink.duckdb.duckdb_comparison_library import (
    exact_match,
    levenshtein_at_thresholds,
)

from pyspark.sql import DataFrame
from splink.spark.spark_linker import SparkLinker
from data_harmonization.main.code.tiger.spark.SparkClass import SparkClass
from data_harmonization.main.resources import config as config_
from data_harmonization.main.resources.log4j import Logger


class Deduplication:
    def __init__(self) -> None:
        self.spark = SparkClass()
        self.logger = Logger(name="deduplication")

    def run(
        self,
        df: Optional[DataFrame] = None,
        blocking_rules: Optional[list[str]] = None,
        comparison_rules: Optional[list[dict]] = None,
    ):
        if not df:
            df = self.spark.read_from_database_to_dataframe(
                config_.raw_entity_table
            )

        if not blocking_rules or len(blocking_rules) == 0:
            # blocking_rules = []
            # for col in df.columns:
            #     blocking_rules.append(f"l.{col} = r.{col}")
            blocking_rules = [
                f"l.{col} = r.{col}" for col in df.columns if col != "id"
            ]

        comparison_rules_prepared = []
        if not comparison_rules or len(comparison_rules) == 0:
            comparison_rules_prepared = [
                exact_match(col)
                for col in df.columns
                if col != "id"
                # levenshtein_at_thresholds(col, [1, 2])
                # for col in df.columns
                # if col != "id"
            ]
        else:
            for rule in comparison_rules:
                # {
                #     "column": "",
                #     "type": "exact / levenshtein",
                #     "threshold": 0.1
                # }

                # exact_match,
                # levenshtein_at_thresholds,
                # jaro_winkler_at_thresholds,
                # jaccard_at_thresholds,

                if str(rule.get("type")).lower() == "exact":
                    comparison_rules_prepared.append(
                        exact_match(rule.get("column"))
                    )
                elif str(rule.get("type")).lower() == "levenshtein":
                    comparison_rules_prepared.append(
                        levenshtein_at_thresholds(
                            str(rule.get("column")),
                            rule.get("threshold", [1, 2]),
                        )
                    )
        settings = {
            "unique_id_column_name": "id",
            "link_type": "dedupe_only",
            "blocking_rules_to_generate_predictions": blocking_rules,
            "comparisons": comparison_rules_prepared
            # "comparisons": [
            #     levenshtein_at_thresholds("first_name", 2),
            #     exact_match("surname"),
            #     exact_match("dob"),
            #     exact_match("city", term_frequency_adjustments=True),
            #     exact_match("email"),
            # ],
        }

        linker = SparkLinker(
            df,
            settings,
            break_lineage_method="parquet",
            num_partitions_on_repartition=80,
        )

        if df.count() > 1000000:
            sampling_rows = 1000000
        else:
            sampling_rows = df.count()

        self.spark.get_sparkSession().sparkContext.setCheckpointDir(
            "data_harmonization/main/code/tiger/spark/spark_checkpoints"
        )
        linker.estimate_u_using_random_sampling(target_rows=sampling_rows)

        for rule in blocking_rules:
            linker.estimate_parameters_using_expectation_maximisation(rule)
        # blocking_rule_for_training = (
        #     "l.first_name = r.first_name and l.surname = r.surname"
        # )
        # linker.estimate_parameters_using_expectation_maximisation(
        #     blocking_rule_for_training
        # )

        # blocking_rule_for_training = "l.dob = r.dob"
        # linker.estimate_parameters_using_expectation_maximisation(
        #     blocking_rule_for_training
        # )

        # blocking_rules_to_generate_predictions are used by Splink when the
        # user called linker.predict().
        pairwise_predictions = linker.predict()
        clusters = linker.cluster_pairwise_predictions_at_threshold(
            pairwise_predictions, threshold_match_probability=0.6
        )

        pairwise_predictions_size = pairwise_predictions.__sizeof__()
        pairwise_predictions_df = pairwise_predictions.as_pandas_dataframe(
            limit=pairwise_predictions_size
        )
        pairwise_predictions_ = self.spark.get_sparkSession().createDataFrame(
            pairwise_predictions_df
        )
        self.spark.write_to_database_from_df(
            table="splink_pairs", df=pairwise_predictions_, mode="overwrite"
        )

        clusters_size = clusters.__sizeof__()
        clusters_df = clusters.as_pandas_dataframe(limit=clusters_size)
        clusters_df_ = self.spark.get_sparkSession().createDataFrame(
            clusters_df
        )
        self.spark.write_to_database_from_df(
            table="splink_clusters", df=clusters_df_, mode="overwrite"
        )


if __name__ == "__main__":
    # comparisons = [
    #     {"column": "title", "type": "exact"},
    #     {"column": "manufacturer", "type": "levenshtein"},
    #     {"column": "price", "type": "levenshtein", "threshold": 2},
    # ]
    # blocking_rules = [
    #     "l.title = r.title or l.manufacturer = r.manufacturer",
    #     "l.price = r.price",
    # ]
    # Deduplication().run(
    #     blocking_rules=blocking_rules, comparison_rules=comparisons
    # )

    Deduplication().run()
