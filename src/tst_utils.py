import pandas as pd


def compare_dataframes(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    compare_cols: list[str]
    ) -> pd.DataFrame:
    """
    Compare two DataFrames on a subset of columns, including duplicate counts.

    Returns rows where:
    - a value combination exists only in df1
    - a value combination exists only in df2
    - a value combination exists in both but with different counts
    """

    # Validate columns
    missing_1 = set(compare_cols) - set(df1.columns)
    missing_2 = set(compare_cols) - set(df2.columns)
    if missing_1 or missing_2:
        raise ValueError(
            f"Missing columns. df1: {missing_1}, df2: {missing_2}"
        )

    # Count occurrences per value combination
    c1 = (
        df1[compare_cols]
        .value_counts()
        .reset_index(name="count_df1")
    )

    c2 = (
        df2[compare_cols]
        .value_counts()
        .reset_index(name="count_df2")
    )

    # Full outer join on the comparison columns
    result = c1.merge(
        c2,
        on=compare_cols,
        how="outer"
    )

    # Replace NaN counts with zero
    result["count_df1"] = result["count_df1"].fillna(0).astype(int)
    result["count_df2"] = result["count_df2"].fillna(0).astype(int)

    # Keep only differences
    result = result[result["count_df1"] != result["count_df2"]]

    # Optional: add classification
    result["difference_type"] = result.apply(
        lambda r:
            "only_in_df1" if r["count_df2"] == 0 else
            "only_in_df2" if r["count_df1"] == 0 else
            "count_mismatch",
        axis=1
    )

    return result.sort_values(compare_cols).reset_index(drop=True)

df1 = pd.DataFrame({
    "A": [1, 2, 3],
    "B": ["Timothy", "Joe", "Alice"],
    "C": ["monthly", "monthly", "yearly"],
    "D": [1992, 1992, 1990],
    "E": ["Ford", "Ford", "BMW"]
})

df2 = pd.DataFrame({
    "B": ["Timothy", "Alice", "Andrew"],
    "C": ["monthly", "yearly", "yearly"],
    "D": [1992, 1990, 1991],
    "E": ["Ford", "BMW", "Toyota"],
    "F": ["x", "y", "z"]
})

compare_cols = ["B", "C", "D", "E"]
compare_cols = ["B", "C", "D", "E"]

out = compare_dataframes(df1, df2, compare_cols)
print(out.to_string())