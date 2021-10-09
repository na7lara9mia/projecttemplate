"""Example function that can be part of domain module."""

import logging
import pandas
import pyspark


def compute_n_vins_per_site(df, site_col):
    """Compute number of vins per site.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing the vehicles produced.
    site_col : string
        Name of the column containing the site information.
    """
    if type(df) == pyspark.sql.dataframe.DataFrame:
        return df.select([site_col, "vin"]).distinct().groupBy(site_col).count().collect()

    elif type(df) == pandas.core.frame.DataFrame:
        return df[["site_code", "vin"]].drop_duplicates().groupby(site_col).size()

    else:
        logging.error((f"The {type(df)} type is not supported. The supported formats are: "
                       "pyspark.sql.dataframe.DataFrame & pandas.core.frame.DataFrame."))
