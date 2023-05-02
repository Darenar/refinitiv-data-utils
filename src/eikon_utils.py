from typing import Union, List, Optional, Dict
import json

import eikon as ek
import pandas as pd
from tqdm import tqdm
import datetime as dt
import dateutil


from .utils import get_end_month_date


def connect(path_to_json_config: str = 'refinitiv-data.config.json'):
    """
    Functions to set-up a connection with Eikon API. Make sure that a workspace is running on your computer.
    
    Parameters
    ----------
    path_to_json_config : str, optional
        Path to the json file with app key, by default 'refinitiv-data.config.json'
    """
    with open(path_to_json_config, 'r') as f:
        config = json.load(f)
    ek.set_app_key(config['sessions']['platform']['rdp']['app-key'])

    
def load_esg(list_of_rics: Union[List[str], str], *args, **kwargs) -> pd.DataFrame:
    """
    Loading Traditional ESG Sore data through get_data eikon api. 
    Additional parameters could be passed through *args, **kwargs arguments.

    Parameters
    ----------
    list_of_rics :
        A separate RIC or a list of RICs

    Returns
    -------
        Data frame with all traditional score available for all RICs provided.
    """
    df, _ = ek.get_data(
        instruments=list_of_rics, 
        fields=[
            'TR.TRESGScore',
            'TR.TRESGScore.date',
            'TR.TRESGScoreGrade',
            'TR.TRESGCScore',
            'TR.TRESGCScoreGrade',
            'TR.EnvironmentPillarScore',
            'TR.SocialPillarScore',
            'TR.GovernancePillarScore',
        ], *args, **kwargs)
    return df
    
def load_pricing(list_of_rics: Union[List[str], str], *args, **kwargs) -> pd.DataFrame:
    """
    Loading pricing data for given RICS through get_data eikon api.
    Additional parameters could be passed through *args, **kwargs arguments.
    
    Parameters
    ----------
    list_of_rics :
        A separate RIC or a list of RICs
    Returns
    -------
        Data frame with all pricing info available for all RICs provided.
    """
    df, _ = ek.get_data(
        instruments=list_of_rics, 
        fields=[
            'TR.PriceClose.date',
            'TR.VOLUME', 
            'TR.HIGH', 
            'TR.LOW', 
            'TR.OPEN', 
            'TR.CLOSE', 
            'TR.COUNT'
        ], *args, **kwargs)
    return df


def load_pricing_series(list_of_rics:  Union[List[str], str], start_date: str, 
                        end_date: str, freq: str = 'daily', batch_size: Optional[int] = None, 
                        fields: Union[List[str], str] = '*') -> pd.DataFrame:
    """
    Loading pricing info for all RICs provided by get_timeseries Eikon api.
    Note - when providing several RICs - the function might return only the date where all of the RICs were available.
    In order to avoid that - one could select a batch_size (usually equal to 1).

    Parameters
    ----------
    list_of_rics : 
        A separate RIC or a list of RICs
    start_date : 
        Start date to load pricing from
    end_date : 
        End date to load pricing until
    freq : optional
        Frequency of the loaded pricing info, by default 'daily'
    batch_size : optional
        Size of the batch used in the loading, by default None
    fields : optional
        Particular fields to load the data for, by default '*'

    Returns
    -------
        Data Frame with pricing time-series for the given RICs
    """
    list_of_rics = list_of_rics if isinstance(list_of_rics, list) else [list_of_rics]        
    batch_size = batch_size if batch_size is not None else len(list_of_rics)
    
    r_dfs = list()
    for b in tqdm(range(0, len(list_of_rics), batch_size)):
        batch_df = ek.get_timeseries(
            rics=list_of_rics[b: b + batch_size],
            fields=fields,
            start_date=start_date,
            end_date=end_date,
            interval=freq
        )
        if batch_size == 1:
            batch_df['RIC'] = list_of_rics[b]
        else:
            batch_df = pd.pivot(pd.melt(batch_df.reset_index(), id_vars=['Date']), index=['Date', 'Security'], columns=['Field'], values='value')
            batch_df = batch_df.reset_index().sort_values(['Security', 'Date']).set_index('Date').rename({'Security': 'RIC'}, axis=1)

        r_dfs.append(batch_df)
    return pd.concat(r_dfs, axis=0)


def load_index_constituents(index_ric: str, *args, **kwargs) -> pd.DataFrame:
    """
    Loading index constituents using get_data Eikon api. 
    Additional parameters could be passed through *args, **kwargs arguments.

    Parameters
    ----------
    index_ric : 
        RIC of the index

    Returns
    -------
        Data frame with index constituents as well as their company names, market caps, and weights.
    """
    # Function to load constituent stocks of a particular index
    sp_df, _ = ek.get_data(
        index_ric, 
        fields=[
            'TR.CompanyMarketCap', 
            'TR.CompanyMarketCap.Date', 
            'TR.IndexConstituentName',
            'TR.IndexConstituentWeightPercent',
            'TR.IndexConstituentRIC'
        ], *args, **kwargs)
    if sp_df['Weight percent'].isna().sum():
        # Calculate total market cap
        total_market_cap_by_date = sp_df.groupby(
            'Date', as_index=False)['Company Market Cap'].sum().rename(
            {'Company Market Cap': 'Total Date Market Cap'}, axis=1)
        sp_df = pd.merge(sp_df, total_market_cap_by_date, how='left', on='Date')
        # Add new column to df with weights
        sp_df.loc[:, 'Weight'] = (sp_df['Company Market Cap'] / sp_df['Total Date Market Cap']) * 100
    return sp_df

    
def load_index_constituents_updates(index_ric: str, parameters: Dict[str, str]) -> pd.DataFrame:
    # Make sure loading both Leavers and Joiners
    if 'IC' in parameters and parameters['IC'] !='B':
        raise ValueError(f"""
            Index Change Type (IC) should always be equal to B (both Joiners and Leavers), got provided with {parameters['IC']}""")
    parameters['IC'] = 'B'
    # To load index constituents, one should provide it without 0#
    index_ric = index_ric.replace('0', '').replace('#', '')
    index_updates_df, _ = ek.get_data(index_ric, fields=[
            'TR.IndexJLConstituentChangeDate',
            'TR.IndexJLConstituentRIC.change',
            'TR.IndexJLConstituentRIC',
            'TR.IndexJLConstituentComName',
            'TR.IndexJLConstituentName'],
        parameters=parameters)
    index_updates_df.loc[:, 'Date'] = pd.to_datetime(index_updates_df['Date']).dt.date
    return index_updates_df


def load_market_cap(ric: str, *args, **kwargs) -> pd.DataFrame:
    """
    Loading market cap info for the RIC using get_data Eikon API.
    Additional parameters could be passed through *args, **kwargs arguments.

    Parameters
    ----------
    ric : str
        Name of the RIC
    Returns
    -------
        Data frame with market caps.
    """
    market_cap_df, _ = ek.get_data(
        ric, 
        fields=[
            'TR.CompanyMarketCap', 
            'TR.CompanyMarketCap.Date'
        ], *args, **kwargs)
    
    if market_cap_df.Date.isna().all():
        # If RIC market cap is not found - try to find without caret ^ symbol suffix.
        market_cap_df, _ = ek.get_data(
            ric.split('^')[0], 
            fields=[
                'TR.CompanyMarketCap', 
                'TR.CompanyMarketCap.Date'
            ], *args, **kwargs)
        return market_cap_df
    market_cap_df.loc[:, 'Date'] = pd.to_datetime(market_cap_df['Date'])
    market_cap_df.sort_values('Date', inplace=True)
    return market_cap_df


def load_index_constituents_historical(index_ric: str, n_months: int = 12 * 40) -> pd.DataFrame:
    """
    Load historical constituents of the index RIC. In brief, the function loads current index state,
    loads leavers and joiners times, and then iteratively constructs time series.

    Parameters
    ----------
    index_ric : str
        RIC of the index
    n_months : int, optional
        Number of months to construct the constituent time series for, by default 12*40

    Returns
    -------
    pd.DataFrame
        Data Frame with monthly state of index constituents
    """
    # Loading current constituents of the index
    current_parts, _ = ek.get_data(index_ric, ['TR.IndexConstituentRIC'])
    current_parts = current_parts['Constituent RIC'].tolist()
    # Get current date
    max_date = dt.date.today()
    min_date = get_end_month_date(max_date-dateutil.relativedelta.relativedelta(months=n_months+1))
    # Load consituent changes
    index_updates_df = load_index_constituents_updates(index_ric, parameters={'sdate': str(max_date), 'edate': str(min_date)})
    # Iteratively, update the index state from month to month
    monthly_constituents = list()
    for _ in range(1, n_months):
        monthly_constituents.append((max_date, list(current_parts))) 
        min_date = get_end_month_date(max_date-dateutil.relativedelta.relativedelta(months=1))
        update_dict = index_updates_df[
            index_updates_df['Date'].between(min_date, max_date, inclusive='right')
        ].groupby('Change')['Constituent RIC'].agg(list).to_dict()
        for leaver in update_dict.get('Leaver', []):
            current_parts.append(leaver)
        for joiner in update_dict.get('Joiner', []):
            current_parts.remove(joiner)
        max_date = min_date
        
    monthly_constituents_df = pd.DataFrame(monthly_constituents, columns=['date', 'constituents'])
    monthly_constituents_df.loc[:, 'num_companies'] = monthly_constituents_df.constituents.str.len()
    return monthly_constituents_df


