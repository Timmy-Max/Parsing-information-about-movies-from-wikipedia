import pandas as pd


def clear_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Clears the dataset of uninformative objects.

    Parameters
    ----------
    data : pd.DataFrame
        Data for cleaning received after parsing.

    Returns
    -------
    Pandas DataFrame
        Cleared Pandas DataFrame with wiki infobox.
    """
    # Replacing the nan values
    data[data["plot"].isnull()] = "no info"
    data[data["genre"].isnull()] = "no info"

    # Replacing the nan values
    data.dropna(subset=["summary"], inplace=True)

    # Throwing out lines with nan in the summary
    data.drop(
        data[data["summary"].str.contains("В эту категорию автоматически")].index,
        inplace=True,
        axis=0
    )

    # Throwing out lines with "no info" in each column
    cond = True
    for col_name in data.columns.values:
        cond &= data[col_name] == "no info"
    data.drop(data[cond].index, axis=0, inplace=True)

    # Resetting indexes
    data.reset_index(drop=True, inplace=True)

    # Rearranging the columns
    return data[["title", "type", "genre", "imdb_rating", "summary", "plot"]]
