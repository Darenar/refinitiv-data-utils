from typing import Tuple, Dict, Union, List

import os
import json
import pandas as pd
import gzip
from pathlib import Path
from zipfile import ZipFile
from pandas.core.indexes.multi import MultiIndex


NestedDict = Dict[str, Union[str, 'NestedDict']]


def read_refinitive_news_dump(path_to_the_dump: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Function to load Refinitiv News dataset. Provides two objects:
    1. Pandas DataFrame with each news entry as a row
    2. Non-news metadata of the file

    Parameters
    ----------
    path_to_the_dump : str
        Path to refinitiv dump file

    Returns
    -------
    Tuple
        DataFrame with news entries and dictionary with metadata
    """
    with gzip.open(path_to_the_dump, 'r') as f:
        json_data = json.load(f)

    meta_data_dict = {k: v for k, v in json_data.items() if k != 'Items'}

    news_df = pd.DataFrame(json_data['Items'])
    news_df = pd.concat([
        news_df[news_df.columns.difference(['timestamps', 'data'])], 
        news_df['timestamps'].apply(pd.Series),
        news_df['data'].apply(pd.Series)], axis=1)
    return news_df, meta_data_dict


def get_hard_drive_folder_path(name_of_hard_drive: str = 'My Passport') -> Path:
    """
    Function to generate the path to the external hard drive on Mac OS 
    
    Parameters
    ----------
    name_of_hard_drive : str
        Name of the hard drive, by default 'My Passport'

    Returns
    -------
    Path
    """
    return Path(os.path.join(*['..' for _ in os.getcwd().split('/')] +  ['Volumes', name_of_hard_drive]))


def list_zip_constituents(path_to_archive: str) -> List[str]:
    """
    Function to list all the consituents from the archive
    
    Parameters
    ----------
    path_to_archive : str
        Path to the archive of interest

    Returns
    -------
    List of constituent names
    """
    with ZipFile(path_to_archive) as z_f:
        return z_f.namelist()


def read_table_from_archive(path_to_archive: str, name_of_the_file: str, *args, **kwards) -> pd.DataFrame:
    """
    Function to read the particular data part from the archive
    Parameters
    ----------
    path_to_archive : str
        Path of the archive file
    name_of_the_file : str
        Name of the file inside the archive to read

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    NotImplementedError
        When the extensions of a consituent file differs neither csv not txt
    """
    with ZipFile(path_to_archive) as z_f:
        if name_of_the_file.endswith('csv'):
            return pd.read_csv(z_f.open(name_of_the_file), *args, **kwards)
        elif name_of_the_file.endswith('txt'):
            return pd.read_table(z_f.open(name_of_the_file), *args, **kwards)
        else:
            raise NotImplementedError(f"Extension {os.path.splitext(name_of_the_file)[-1]} is not supported")
    return None


def multi_index_to_dict(df: pd.DataFrame) -> NestedDict:
    """
    The function that transforms pandas dataframe with multi index into a dictionary

    Parameters
    ----------
    df : pd.DataFrame
        Data frame with a multiindex

    Returns
    -------
        Dictionary from multiindex
    """
    # If the index is a multiindex - recursivelly extract top_level and repeat the function on low-level
    if isinstance(df.index, MultiIndex):
        return {k: multi_index_to_dict(df.loc[k]) for k in df.index.remove_unused_levels().levels[0]}
    # return {k: df.to_dict(orient='records') for k in df.index}
    return df.to_dict('index')