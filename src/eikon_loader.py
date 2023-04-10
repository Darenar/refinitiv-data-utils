from typing import Union, List, Optional
import json

import eikon as ek
import pandas as pd
from tqdm import tqdm


class EikonLoader:
    
    @staticmethod
    def connect(path_to_json_config: str = 'refinitiv-data.config.json'):
        with open(path_to_json_config, 'r') as f:
            config = json.load(f)
        ek.set_app_key(config['sessions']['platform']['rdp']['app-key'])
    
    @staticmethod
    def load_esg(list_of_rics: Union[List[str], str], start_date: str, end_date: str, freq: str = 'Y') -> pd.DataFrame:
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
            ], 
            parameters={
                'sdate': start_date,
                'edate': end_date,
                'frq': freq
            })
        return df

    def load_pricing(list_of_rics: Union[List[str], str], start_date: str, end_date: str, freq: str = 'D') -> pd.DataFrame:
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
            ], 
            parameters={
                'sdate': start_date,
                'edate': end_date,
                'frq': freq
            })
        return df

    @staticmethod
    def load_pricing_series(list_of_rics:  Union[List[str], str], start_date: str, end_date: str, freq: str = 'daily', batch_size: Optional[int] = None) -> pd.DataFrame:
        list_of_rics = list_of_rics if isinstance(list_of_rics, list) else [list_of_rics]        
        batch_size = batch_size if batch_size is not None else len(list_of_rics)
        r_dfs = list()
        for b in tqdm(range(0, len(list_of_rics), batch_size)):
            batch_df = ek.get_timeseries(
                rics=list_of_rics[b: b + batch_size],
                fields='*',
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
        

    @staticmethod
    def load_index_constituents(index_ric: str) -> pd.DataFrame:
        """
        Function to load constituent stocks of a particular index
        """
        sp_df, _ = ek.get_data(
            index_ric, 
            fields=[
                'TR.CompanyMarketCap', 
                'TR.CompanyMarketCap.Date', 
                'TR.IndexConstituentName',
                'TR.IndexConstituentWeightPercent',
                'TR.IndexConstituentRIC'
            ])
        if sp_df['Weight percent'].isna().sum():
            # Calculate total market cap
            total_market_cap = sp_df['Company Market Cap'].sum()

            # Add new column to df with weights
            sp_df.loc[:,'Weight'] = (sp_df['Company Market Cap']/total_market_cap) * 100
        return sp_df



