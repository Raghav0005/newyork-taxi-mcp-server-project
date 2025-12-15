import pandas as pd
from pathlib import Path
from typing import Tuple

class TaxiDataLoader:
    def __init__(self, data_dir: str = 'data'):
        self.data_dir = Path(data_dir)
        self.df_green = None
        self.df_yellow = None
        self.zone_lookup = None
    
    def load_all_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        self.df_green = pd.read_parquet(self.data_dir / 'green_tripdata_2025-01.parquet')
        self.df_yellow = pd.read_parquet(self.data_dir / 'yellow_tripdata_2025-01.parquet')
        self.zone_lookup = pd.read_csv(self.data_dir / 'taxi_zone_lookup.csv')
        
        self._clean_zone_lookup()
        self._merge_zone_info()
        self._add_temporal_features()
        
        return self.df_green, self.df_yellow, self.zone_lookup
    
    def _clean_zone_lookup(self):
        self.zone_lookup.loc[self.zone_lookup['Zone'].isnull(), 'Zone'] = 'Unknown'
        self.zone_lookup.loc[self.zone_lookup['Borough'].isnull(), 'Borough'] = 'Unknown'
    
    def _merge_zone_info(self):
        zone_cols = self.zone_lookup[['LocationID', 'Borough', 'Zone']]
        for df in [self.df_green, self.df_yellow]:
            for prefix, id_col in [('PU', 'PULocationID'), ('DO', 'DOLocationID')]:
                df_temp = df.merge(zone_cols, left_on=id_col, right_on='LocationID', how='left')
                df[f'{prefix}_Borough'] = df_temp['Borough']
                df[f'{prefix}_Zone'] = df_temp['Zone']
    
    def _add_temporal_features(self):
        for df, taxi_type in [(self.df_green, 'green'), (self.df_yellow, 'yellow')]:
            datetime_col = 'lpep_pickup_datetime' if taxi_type == 'green' else 'tpep_pickup_datetime'
            
            df['pickup_datetime'] = pd.to_datetime(df[datetime_col])
            df['hour'] = df['pickup_datetime'].dt.hour
            df['day_of_week'] = df['pickup_datetime'].dt.day_name()
            df['date'] = df['pickup_datetime'].dt.date
            
            is_weekday = ~df['day_of_week'].isin(['Saturday', 'Sunday'])
            is_peak_hour = ((df['hour'] >= 7) & (df['hour'] <= 10)) | ((df['hour'] >= 16) & (df['hour'] <= 20))
            df['period'] = 'Off-Peak'
            df.loc[is_weekday & is_peak_hour, 'period'] = 'Peak'

def get_df(df_green: pd.DataFrame, df_yellow: pd.DataFrame, taxi_type: str) -> pd.DataFrame:
    dfs = {'green': df_green, 'yellow': df_yellow}
    return dfs[taxi_type]
