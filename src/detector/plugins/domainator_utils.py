import numpy as np
import itertools
import pylcs
import Levenshtein


def strip_domain(query: str):
    """Extract the domain name from the message for the window grouping

    Currently does not differentiate between messages coming from
    different users.

    Returns:
        str: Domain name string that the window will be grouped by
    """

    query = query.strip(".")
    query = query.split(".")

    domain = ""

    if len(query) >= 2:
        domain = query[-2]

    return domain

def get_domainator_features(queries: list) -> np.ndarray:
    """Extracts feature vector from domain name for ML model inference.

    Computes various statistical and linguistic features from the domain name
    including label lengths, character frequencies, entropy measures, and
    counts of different character types across domain name levels.

    Args:
        queries (list): List of query strings to extract features from.

    Returns:
        numpy.ndarray: Feature vector ready for ML model prediction.
    """

    queries = [query.strip(".") for query in queries]
    subdomains = [".".join(domain.split(".")[:-2]) for domain in queries]

    # Values can be put directly into an array, as the return converts them anyway,
    # but this slightly improves readability
    metrics = {
        "levenshtein": [],
        "jaro": [],
        "rev_jaro": [],
        "jaro_winkler": [],
        "rev_jaro_wink": [],
        "lcs_seq": [],
        "lcs_str": [],
    }

    # if subdomains:
    cartesian = list(itertools.combinations(subdomains, 2))

    metrics["levenshtein"] = np.mean(
        [Levenshtein.ratio(product[0], product[1]) for product in cartesian]
    )
    metrics["jaro"] = np.mean(
        [Levenshtein.jaro(product[0], product[1]) for product in cartesian]
    )
    metrics["jaro_winkler"] = np.mean(
        [
            Levenshtein.jaro_winkler(product[0], product[1], prefix_weight=0.2)
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
                product[0][::-1], product[1][::-1], prefix_weight=0.2
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

    return np.fromiter(metrics.values(), dtype=float).reshape(1, -1)