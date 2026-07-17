from string import ascii_lowercase as alc
from typing import List

import math
import polars as pl

import numpy as np
import itertools
import pylcs
import Levenshtein

from src.base.log_config import get_logger

logger = get_logger("train.feature")


class Processor:
    """Extracts statistical and linguistic features from domain name datasets.

    Computes comprehensive feature sets including domain label statistics, character
    frequencies, entropy measures, and domain structure analysis for machine learning
    model training and DGA detection tasks.
    """

    def __init__(self, features_to_drop: List) -> None:
        """
        Args:
            features_to_drop (List): List of column names to exclude from final features.
        """
        self.features_to_drop = features_to_drop

    def transform(self, x: pl.DataFrame) -> pl.DataFrame:
        """Extracts comprehensive feature set from domain name dataset.

        Computes domain label statistics, character frequencies for all letters,
        character type ratios, and entropy measures for different domain levels.
        Handles missing values and removes specified columns from final output.

        Args:
            x (pl.DataFrame): Input dataset with domain structure columns.

        Returns:
            pl.DataFrame: Feature-engineered dataset ready for ML model training.
        """
        logger.debug("Start data transformation")
        x = x.with_columns(
            [
                (pl.col("query").str.split(".").list.len().alias("label_length")),
                (
                    pl.col("query")
                    .str.split(".")
                    .list.max()
                    .str.len_chars()
                    .alias("label_max")
                ),
                (
                    pl.col("query")
                    .str.strip_chars(".")
                    .str.len_chars()
                    .alias("label_average")
                ),
            ]
        )
        logger.debug("Get letter frequency")
        for i in alc:
            x = x.with_columns(
                [
                    (
                        pl.col("query")
                        .str.to_lowercase()
                        .str.count_matches(rf"{i}")
                        .truediv(pl.col("query").str.len_chars())
                    ).alias(f"freq_{i}"),
                ]
            )
        logger.debug("Get full, alpha, special, and numeric count.")
        for level in ["thirdleveldomain", "secondleveldomain", "fqdn"]:
            x = x.with_columns(
                [
                    (
                        pl.when(pl.col(level).str.len_chars().eq(0))
                        .then(pl.lit(0))
                        .otherwise(
                            pl.col(level)
                            .str.len_chars()
                            .truediv(pl.col(level).str.len_chars())
                        )
                    ).alias(f"{level}_full_count"),
                    (
                        pl.when(pl.col(level).str.len_chars().eq(0))
                        .then(pl.lit(0))
                        .otherwise(
                            pl.col(level)
                            .str.count_matches(r"[a-zA-Z]")
                            .truediv(pl.col(level).str.len_chars())
                        )
                    ).alias(f"{level}_alpha_count"),
                    (
                        pl.when(pl.col(level).str.len_chars().eq(0))
                        .then(pl.lit(0))
                        .otherwise(
                            pl.col(level)
                            .str.count_matches(r"[0-9]")
                            .truediv(pl.col(level).str.len_chars())
                        )
                    ).alias(f"{level}_numeric_count"),
                    (
                        pl.when(pl.col(level).str.len_chars().eq(0))
                        .then(pl.lit(0))
                        .otherwise(
                            pl.col(level)
                            .str.count_matches(r"[^\w\s]")
                            .truediv(pl.col(level).str.len_chars())
                        )
                    ).alias(f"{level}_special_count"),
                ]
            )

        logger.debug("Start entropy calculation")
        for ent in ["fqdn", "thirdleveldomain", "secondleveldomain"]:
            x = x.with_columns(
                [
                    (
                        pl.col(ent).map_elements(
                            lambda x: [
                                float(str(x).count(c)) / len(str(x))
                                for c in dict.fromkeys(list(str(x)))
                            ],
                            return_dtype=pl.List(pl.Float64),
                        )
                    ).alias("prob"),
                ]
            )

            t = math.log(2.0)

            x = x.with_columns(
                [
                    (
                        pl.col("prob")
                        .list.eval(-pl.element() * pl.element().log() / t)
                        .list.sum()
                    ).alias(f"{ent}_entropy"),
                ]
            )
            x = x.drop("prob")
        logger.debug("Finished entropy calculation")

        logger.debug("Fill NaN.")
        x = x.fill_nan(0)

        logger.debug("Drop features that are not useful.")
        x = x.drop(self.features_to_drop)

        logger.debug("Finished data transformation")

        logger.info("Finished data transformation")

        return x

    def transform_domainator(self, x: pl.DataFrame) -> pl.DataFrame:
        logger.debug("Domainator transform")

        metrics_list = []
        window_size = 10
        min_window_size = 3

        x = x.with_columns(
            pl.concat_str(
                [pl.col("secondleveldomain"), pl.col("tld")], separator="."
            ).alias("domain")
        )

        for user in x["user"].unique():
            # logger.debug(x.filter(pl.col('user') == user))
            for domain in x.filter(pl.col("user") == user)["domain"].unique():
                sub_list = x.filter(
                    (pl.col("user") == user) & (pl.col("domain") == domain)
                )["thirdleveldomain"]
                true_class = x.filter(pl.col("domain") == domain)[
                    "class"
                ].unique()  # currently assumes domain is not both malicious and legitimate

                windows = [
                    sub_list[i : i + window_size]
                    for i in range(0, len(sub_list), window_size)
                ]

                if not windows:
                    windows = sub_list

                for item in windows:
                    if len(item) > min_window_size:
                        cartesian = list(itertools.combinations(item, 2))

                        metrics = {
                            "user": user,
                            "class": true_class[0],
                            "query": domain,
                            "levenshtein": [],
                            "jaro": [],
                            "rev_jaro": [],
                            "jaro_winkler": [],
                            "rev_jaro_wink": [],
                            "lcs_seq": [],
                            "lcs_str": [],
                        }

                        metrics["levenshtein"] = np.mean(
                            [
                                Levenshtein.ratio(product[0], product[1])
                                for product in cartesian
                            ]
                        )
                        metrics["jaro"] = np.mean(
                            [
                                Levenshtein.jaro(product[0], product[1])
                                for product in cartesian
                            ]
                        )
                        metrics["jaro_winkler"] = np.mean(
                            [
                                Levenshtein.jaro_winkler(
                                    product[0], product[1], prefix_weight=0.2
                                )
                                for product in cartesian
                            ]
                        )
                        metrics["rev_jaro"] = np.mean(
                            [
                                Levenshtein.jaro(product[0][::-1], product[1][::-1])
                                for product in cartesian
                            ]
                        )
                        metrics["rev_jaro_wink"] = np.mean(
                            [
                                Levenshtein.jaro_winkler(
                                    product[0][::-1],
                                    product[1][::-1],
                                    prefix_weight=0.2,
                                )
                                for product in cartesian
                            ]
                        )

                        metrics["lcs_seq"] = np.mean(
                            [
                                (
                                    pylcs.lcs_sequence_length(product[0], product[1])
                                    / ((len(product[0]) + len(product[1])) / 2)
                                    if len(product[0]) and len(product[1])
                                    else 0.0
                                )
                                for product in cartesian
                            ]
                        )
                        metrics["lcs_str"] = np.mean(
                            [
                                (
                                    pylcs.lcs_string_length(product[0], product[1])
                                    / ((len(product[0]) + len(product[1])) / 2)
                                    if len(product[0]) and len(product[1])
                                    else 0.0
                                )
                                for product in cartesian
                            ]
                        )

                        metrics_list.append(metrics)

        df = pl.from_dicts(metrics_list)

        logger.debug(df)
        logger.debug(df["class"].unique())

        df = df.drop(["user"])

        logger.debug("Transform done")
        return df
