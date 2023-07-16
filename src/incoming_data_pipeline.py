import pandas as pd
import numpy as np

from tqdm import tqdm

from collections import Counter

from typing import List, Dict


NUM_DIGITS = 19
NANOSEC_DIFF = 9
HOUR_DIFF = 3600
TIMESTAMP_COL = "unix"

COLUMNS_TO_INCLUDE = [
    "unix",
    "open",
    "high",
    "low",
    "close",
    "Volume ETH",
    "Volume USD",
]
DATE_FIELD = "date"
SYMBOL_COL = "symbol"
SYMBOL = "ETH/USD"

COLUMNS_TO_REINDEX = [
    "unix",
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "Volume ETH",
    "Volume USD",
]
numeric_columns = ["unix", "open", "high", "low", "close", "Volume ETH", "Volume USD"]


def remove_first_line(filename_in: str, filename_out: str) -> None:
    with open(filename_in, "r") as f_read, open(filename_out, "w") as f_write:
        for i, line in tqdm(enumerate(f_read.readlines())):
            if i == 0:
                continue
            else:
                f_write.write(f"{line}")


def align_timestamps(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Aligns all of the timestamps to the same format
    """

    dataframe[TIMESTAMP_COL] = (
        dataframe[TIMESTAMP_COL].astype(str).str.ljust(NUM_DIGITS, "0")
    )
    dataframe[TIMESTAMP_COL] = dataframe[TIMESTAMP_COL].astype(np.int64)
    dataframe = dataframe.sort_values(TIMESTAMP_COL, ascending=False)
    return dataframe


convert_to_seconds = lambda val: int(val / 10**9)


### Check 1
def check_interpolation_needed(dataframe: pd.DataFrame) -> bool:
    """
    Checks if all of the differences between data points are in place
    :param dataframe: a dataframe to check difference between data points

    :returns bool: whether the interpolation is needed or not
    """

    diffs = [
        int(
            (dataframe[TIMESTAMP_COL].iloc[i] - dataframe[TIMESTAMP_COL].iloc[i + 1])
            / 10**NANOSEC_DIFF
        )
        for i in range(dataframe.shape[0] - 1)
    ]
    diff_counter = dict(Counter(diffs))
    keys_in_counter = list(diff_counter.keys())
    keys_in_counter = [abs(val) for val in keys_in_counter]

    idx = np.where(np.array(diffs) != HOUR_DIFF)

    return not (len(keys_in_counter) == 1 and HOUR_DIFF in keys_in_counter), idx


def calc_values(next_value: pd.Series, current_value: pd.Series) -> List[Dict]:
    """
    Returns the list of dictionaries of interpolated values between originally
    adjacent in a database calculated in between those values

    :param next_value: a row series of the right bound
    :param current_value: a row series of the left bound

    :returns values_to_return: a list of rows that are calculated in between (interpolated)
    """
    values_to_return = []

    dict_diff = current_value - next_value
    n_vals_to_insert = int(
        convert_to_seconds(current_value["unix"] - next_value["unix"]) / 3600
    )
    for i in range(n_vals_to_insert - 1):
        calculated_value = dict((i + 1) * dict_diff / n_vals_to_insert + next_value)
        values_to_return.append(calculated_value)

    return values_to_return


def interpolate_missing(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Interpolates on the whole dataframe the values if there are 'holes' in the sequence data

    :param dataframe: the original data to interpolate
    :returns dataframe: the dataframe with the added values
    """

    values = []
    next_value_full = None
    for i in tqdm(range(dataframe.shape[0] - 1)):
        current_value_full = dataframe.loc[i]
        next_value_full = dataframe.loc[i + 1]

        current_value = dataframe.loc[i, numeric_columns]
        next_value = dataframe.loc[i + 1, numeric_columns]

        values.append(current_value_full)

        if convert_to_seconds(current_value[TIMESTAMP_COL] - next_value[TIMESTAMP_COL]) != 3600:
            values_to_append = calc_values(next_value, current_value)
            for value in values_to_append:
                value[DATE_FIELD] = pd.to_datetime(np.int64(value[TIMESTAMP_COL])).strftime("%Y-%m-%d %H:%M:%S")
                # datetime.strptime(current_value['unix'], "%Y-%m-%d %H:%M:%S")
                value[SYMBOL_COL] = SYMBOL
                value = pd.Series(value).reindex(COLUMNS_TO_REINDEX)
                values.append(value)

    # Last one
    values.append(next_value_full)
    new_df = pd.DataFrame(values)

    return new_df


def prepare_raw_data(df: pd.DataFrame):
    df = align_timestamps(df)
    df_new = df

    if not df.empty:
        assert set([len(str(value)) for value in df["unix"]]) == set([NUM_DIGITS])

        if check_interpolation_needed(df)[0]:
            df_new = interpolate_missing(df)
            df_new = df_new.sort_values("unix", ascending=True)
            df_new["unix"] = df_new.unix.astype(np.int64)

        # TODO: logging here
        check_needed, idxs_failed = check_interpolation_needed(df_new)
        assert not check_needed, "Interpolation failed"

    return df_new
