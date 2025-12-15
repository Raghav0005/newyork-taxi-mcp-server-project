import json
import pandas as pd
from typing import Optional, Literal, List
from data_loader import get_df

def _get_taxi_types(taxi_type: Literal['green', 'yellow', 'both']) -> List[str]:
    return ['green', 'yellow'] if taxi_type == 'both' else [taxi_type]

def get_trip_volume_by_hour(
    df_green: pd.DataFrame,
    df_yellow: pd.DataFrame,
    taxi_type: Literal['green', 'yellow', 'both'] = 'both',
    hour: Optional[int] = None
) -> str:
    if hour is not None and not (0 <= hour <= 23):
        return json.dumps({'error': 'Hour must be between 0 and 23'})
    
    results = {}
    for tt in _get_taxi_types(taxi_type):
        hourly_counts = get_df(df_green, df_yellow, tt).groupby('hour').size()
        results[tt] = ({'hour': hour, 'trip_count': int(hourly_counts.get(hour, 0))} if hour is not None
                      else {int(h): int(c) for h, c in hourly_counts.items()})
    
    return json.dumps(results, indent=2)

def get_trip_volume_by_day(
    df_green: pd.DataFrame,
    df_yellow: pd.DataFrame,
    taxi_type: Literal['green', 'yellow', 'both'] = 'both',
    day_of_week: Optional[str] = None
) -> str:
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    results = {}
    
    for tt in _get_taxi_types(taxi_type):
        daily_counts = get_df(df_green, df_yellow, tt).groupby('day_of_week').size()
        
        if day_of_week:
            day_title = day_of_week.title()
            if day_title not in daily_counts.index:
                return json.dumps({'error': f'Invalid day: {day_of_week}'})
            results[tt] = {'day': day_title, 'trip_count': int(daily_counts[day_title])}
        else:
            results[tt] = {day: int(daily_counts.get(day, 0)) for day in day_order}
    
    return json.dumps(results, indent=2)

def get_peak_vs_offpeak_stats(
    df_green: pd.DataFrame,
    df_yellow: pd.DataFrame,
    taxi_type: Literal['green', 'yellow', 'both'] = 'both'
) -> str:
    results = {}
    
    for tt in _get_taxi_types(taxi_type):
        df = get_df(df_green, df_yellow, tt)
        period_stats = df.groupby('period').agg({
            'fare_amount': ['count', 'mean', 'median'],
            'trip_distance': ['mean', 'median']
        }).round(2)
        
        results[tt] = {
            period: {
                'trip_count': int(period_stats.loc[period, ('fare_amount', 'count')]),
                'avg_fare': float(period_stats.loc[period, ('fare_amount', 'mean')]),
                'median_fare': float(period_stats.loc[period, ('fare_amount', 'median')]),
                'avg_distance': float(period_stats.loc[period, ('trip_distance', 'mean')]),
                'median_distance': float(period_stats.loc[period, ('trip_distance', 'median')])
            }
            for period in ['Peak', 'Off-Peak'] if period in period_stats.index
        }
        
        period_counts = df['period'].value_counts()
        results[tt]['distribution'] = {
            period: {'count': int(count), 'percentage': round(100 * count / len(df), 1)}
            for period, count in period_counts.items()
        }
    
    return json.dumps(results, indent=2)

def get_top_pickup_zones(
    df_green: pd.DataFrame,
    df_yellow: pd.DataFrame,
    taxi_type: Literal['green', 'yellow', 'both'] = 'both',
    top_n: int = 10
) -> str:
    results = {}
    
    for tt in _get_taxi_types(taxi_type):
        df = get_df(df_green, df_yellow, tt)
        top_zones = df['PU_Zone'].value_counts().head(top_n)
        results[tt] = [{'rank': i + 1, 'zone': zone, 'trip_count': int(count), 
                        'percentage': round(100 * count / len(df), 2)}
                       for i, (zone, count) in enumerate(top_zones.items())]
    
    return json.dumps(results, indent=2)

def get_top_dropoff_zones(
    df_green: pd.DataFrame,
    df_yellow: pd.DataFrame,
    taxi_type: Literal['green', 'yellow', 'both'] = 'both',
    top_n: int = 10
) -> str:
    results = {}
    
    for tt in _get_taxi_types(taxi_type):
        df = get_df(df_green, df_yellow, tt)
        top_zones = df['DO_Zone'].value_counts().head(top_n)
        results[tt] = [{'rank': i + 1, 'zone': zone, 'trip_count': int(count),
                        'percentage': round(100 * count / len(df), 2)}
                       for i, (zone, count) in enumerate(top_zones.items())]
    
    return json.dumps(results, indent=2)

def get_fare_statistics(
    df_green: pd.DataFrame,
    df_yellow: pd.DataFrame,
    taxi_type: Literal['green', 'yellow', 'both'] = 'both',
    period: Optional[Literal['Peak', 'Off-Peak']] = None,
    hour: Optional[int] = None
) -> str:
    if hour is not None and not (0 <= hour <= 23):
        return json.dumps({'error': 'Hour must be between 0 and 23'})
    
    results = {}
    
    for tt in _get_taxi_types(taxi_type):
        df = get_df(df_green, df_yellow, tt)
        if period:
            df = df[df['period'] == period]
        if hour is not None:
            df = df[df['hour'] == hour]
        
        fares = df[(df['fare_amount'] > 0) & (df['fare_amount'] <= 200)]['fare_amount']
        
        if len(fares) == 0:
            results[tt] = {'error': 'No data matching filters'}
        else:
            results[tt] = {
                'trip_count': len(fares), 'mean': round(float(fares.mean()), 2),
                'median': round(float(fares.median()), 2), 'std': round(float(fares.std()), 2),
                'min': round(float(fares.min()), 2), 'max': round(float(fares.max()), 2),
                'q25': round(float(fares.quantile(0.25)), 2), 'q75': round(float(fares.quantile(0.75)), 2)
            }
            if period:
                results[tt]['filter_period'] = period
            if hour is not None:
                results[tt]['filter_hour'] = hour
    
    return json.dumps(results, indent=2)

def _aggregate_fares(df_green, df_yellow, taxi_type, groupby_col):
    results = {}
    for tt in _get_taxi_types(taxi_type):
        df = get_df(df_green, df_yellow, tt)
        fares = df[(df['fare_amount'] > 0) & (df['fare_amount'] <= 200)]
        agg = fares.groupby(groupby_col)['fare_amount'].agg(['mean', 'median', 'count'])
        results[tt] = {
            (int(k) if isinstance(k, (int, float)) else k): {
                'avg_fare': round(float(row['mean']), 2),
                'median_fare': round(float(row['median']), 2),
                'trip_count': int(row['count'])
            }
            for k, row in agg.iterrows()
        }
    return json.dumps(results, indent=2)

def get_fares_by_hour(df_green, df_yellow, taxi_type='both'):
    return _aggregate_fares(df_green, df_yellow, taxi_type, 'hour')

def get_fares_by_day(df_green, df_yellow, taxi_type='both'):
    return _aggregate_fares(df_green, df_yellow, taxi_type, 'day_of_week')

def get_fares_by_period(df_green, df_yellow, taxi_type='both'):
    return _aggregate_fares(df_green, df_yellow, taxi_type, 'period')

def get_popular_routes(
    df_green: pd.DataFrame,
    df_yellow: pd.DataFrame,
    taxi_type: Literal['green', 'yellow', 'both'] = 'both',
    top_n: int = 10
) -> str:
    results = {}
    
    for tt in _get_taxi_types(taxi_type):
        routes = get_df(df_green, df_yellow, tt).groupby(['PU_Zone', 'DO_Zone']).agg({
            'fare_amount': ['count', 'mean'], 'trip_distance': 'mean'
        }).round(2)
        routes.columns = ['trip_count', 'avg_fare', 'avg_distance']
        routes = routes.sort_values('trip_count', ascending=False).head(top_n)
        
        results[tt] = [{'rank': i + 1, 'pickup_zone': pu, 'dropoff_zone': do,
                        'trip_count': int(row['trip_count']), 'avg_fare': float(row['avg_fare']),
                        'avg_distance': float(row['avg_distance'])}
                       for i, ((pu, do), row) in enumerate(routes.iterrows())]
    
    return json.dumps(results, indent=2)

def compare_taxi_types(
    df_green: pd.DataFrame,
    df_yellow: pd.DataFrame,
    metric: Literal['trip_volume', 'avg_fare', 'avg_distance', 'peak_distribution'] = 'trip_volume'
) -> str:
    results = {'metric': metric, 'green': {}, 'yellow': {}, 'comparison': {}}
    
    if metric == 'trip_volume':
        g_cnt, y_cnt = len(df_green), len(df_yellow)
        results['green']['total_trips'] = g_cnt
        results['yellow']['total_trips'] = y_cnt
        results['comparison'] = {'ratio': round(y_cnt / g_cnt, 2), 'difference': y_cnt - g_cnt}
        
    elif metric in ['avg_fare', 'avg_distance']:
        col = 'fare_amount' if metric == 'avg_fare' else 'trip_distance'
        max_val = 200 if metric == 'avg_fare' else 50
        g_data = df_green[(df_green[col] > 0) & (df_green[col] <= max_val)][col]
        y_data = df_yellow[(df_yellow[col] > 0) & (df_yellow[col] <= max_val)][col]
        
        for tt, data in [('green', g_data), ('yellow', y_data)]:
            results[tt] = {'mean': round(float(data.mean()), 2), 'median': round(float(data.median()), 2)}
        results['comparison']['mean_difference'] = round(results['yellow']['mean'] - results['green']['mean'], 2)
        
    elif metric == 'peak_distribution':
        for tt, df in [('green', df_green), ('yellow', df_yellow)]:
            period_pct = df['period'].value_counts(normalize=True) * 100
            results[tt] = {'Peak': round(float(period_pct.get('Peak', 0)), 1),
                          'Off-Peak': round(float(period_pct.get('Off-Peak', 0)), 1)}
        results['comparison']['peak_difference'] = round(results['yellow']['Peak'] - results['green']['Peak'], 1)
    
    return json.dumps(results, indent=2)

def get_dataset_summary(
    df_green: pd.DataFrame,
    df_yellow: pd.DataFrame,
    zone_lookup: pd.DataFrame
) -> str:
    def taxi_summary(df):
        return {
            'total_trips': len(df),
            'unique_pickup_zones': int(df['PU_Zone'].nunique()),
            'unique_dropoff_zones': int(df['DO_Zone'].nunique()),
            'date_range': f"{df['date'].min()} to {df['date'].max()}"
        }
    
    return json.dumps({
        'data_period': 'January 2025',
        'green_taxi': taxi_summary(df_green),
        'yellow_taxi': taxi_summary(df_yellow),
        'total_zones': len(zone_lookup)
    }, indent=2)

def get_zones_by_time(
    df_green: pd.DataFrame,
    df_yellow: pd.DataFrame,
    taxi_type: Literal['green', 'yellow', 'both'] = 'both',
    zone_type: Literal['pickup', 'dropoff'] = 'pickup',
    day_of_week: Optional[str] = None,
    hour: Optional[int] = None,
    period: Optional[Literal['Peak', 'Off-Peak']] = None,
    top_n: int = 10
) -> str:
    results = {}
    zone_col = 'PU_Zone' if zone_type == 'pickup' else 'DO_Zone'
    
    for tt in _get_taxi_types(taxi_type):
        df = get_df(df_green, df_yellow, tt)
        
        # Apply filters
        if day_of_week:
            day_title = day_of_week.title()
            df = df[df['day_of_week'] == day_title]
        
        if hour is not None:
            if 0 <= hour <= 23:
                df = df[df['hour'] == hour]
            else:
                return json.dumps({'error': 'Hour must be between 0 and 23'})
        
        if period:
            df = df[df['period'] == period]
        
        if len(df) == 0:
            results[tt] = {'error': 'No trips matching the specified filters'}
            continue
        
        # Get top zones
        top_zones = df[zone_col].value_counts().head(top_n)
        
        results[tt] = {
            'filters': {
                'day_of_week': day_of_week,
                'hour': hour,
                'period': period,
                'zone_type': zone_type
            },
            'total_trips_matching_filters': len(df),
            'zones': [
                {
                    'rank': i + 1,
                    'zone': zone,
                    'trip_count': int(count),
                    'percentage_of_filtered': round(100 * count / len(df), 2)
                }
                for i, (zone, count) in enumerate(top_zones.items())
            ]
        }
    
    return json.dumps(results, indent=2)

def search_trips(
    df_green: pd.DataFrame,
    df_yellow: pd.DataFrame,
    taxi_type: Literal['green', 'yellow', 'both'] = 'both',
    min_fare: Optional[float] = None,
    max_fare: Optional[float] = None,
    min_distance: Optional[float] = None,
    max_distance: Optional[float] = None,
    pickup_zone: Optional[str] = None,
    dropoff_zone: Optional[str] = None,
    day_of_week: Optional[str] = None,
    hour: Optional[int] = None,
    period: Optional[Literal['Peak', 'Off-Peak']] = None
) -> str:
    results = {}
    
    for tt in _get_taxi_types(taxi_type):
        df = get_df(df_green, df_yellow, tt)
        
        # Apply all filters
        if min_fare is not None:
            df = df[df['fare_amount'] >= min_fare]
        if max_fare is not None:
            df = df[df['fare_amount'] <= max_fare]
        if min_distance is not None:
            df = df[df['trip_distance'] >= min_distance]
        if max_distance is not None:
            df = df[df['trip_distance'] <= max_distance]
        if pickup_zone:
            df = df[df['PU_Zone'].str.contains(pickup_zone, case=False, na=False)]
        if dropoff_zone:
            df = df[df['DO_Zone'].str.contains(dropoff_zone, case=False, na=False)]
        if day_of_week:
            df = df[df['day_of_week'] == day_of_week.title()]
        if hour is not None:
            if 0 <= hour <= 23:
                df = df[df['hour'] == hour]
            else:
                return json.dumps({'error': 'Hour must be between 0 and 23'})
        if period:
            df = df[df['period'] == period]
        
        if len(df) == 0:
            results[tt] = {'error': 'No trips matching the specified criteria'}
            continue
        
        # Calculate statistics
        results[tt] = {
            'filters_applied': {
                'min_fare': min_fare,
                'max_fare': max_fare,
                'min_distance': min_distance,
                'max_distance': max_distance,
                'pickup_zone': pickup_zone,
                'dropoff_zone': dropoff_zone,
                'day_of_week': day_of_week,
                'hour': hour,
                'period': period
            },
            'matching_trips': len(df),
            'statistics': {
                'avg_fare': round(float(df['fare_amount'].mean()), 2),
                'median_fare': round(float(df['fare_amount'].median()), 2),
                'avg_distance': round(float(df['trip_distance'].mean()), 2),
                'median_distance': round(float(df['trip_distance'].median()), 2),
                'total_revenue': round(float(df['fare_amount'].sum()), 2)
            },
            'top_pickup_zones': df['PU_Zone'].value_counts().head(5).to_dict(),
            'top_dropoff_zones': df['DO_Zone'].value_counts().head(5).to_dict()
        }
    
    return json.dumps(results, indent=2)

def get_borough_analysis(
    df_green: pd.DataFrame,
    df_yellow: pd.DataFrame,
    taxi_type: Literal['green', 'yellow', 'both'] = 'both',
    borough: Optional[str] = None,
    analysis_type: Literal['pickup', 'dropoff', 'both'] = 'both'
) -> str:
    results = {}
    
    for tt in _get_taxi_types(taxi_type):
        df = get_df(df_green, df_yellow, tt)
        
        if borough:
            borough_title = borough.title()
            
            if analysis_type in ['pickup', 'both']:
                pickup_df = df[df['PU_Borough'] == borough_title]
                pickup_stats = {
                    'trip_count': len(pickup_df),
                    'avg_fare': round(float(pickup_df['fare_amount'].mean()), 2) if len(pickup_df) > 0 else 0,
                    'avg_distance': round(float(pickup_df['trip_distance'].mean()), 2) if len(pickup_df) > 0 else 0,
                    'top_destinations': pickup_df['DO_Zone'].value_counts().head(5).to_dict() if len(pickup_df) > 0 else {}
                }
            
            if analysis_type in ['dropoff', 'both']:
                dropoff_df = df[df['DO_Borough'] == borough_title]
                dropoff_stats = {
                    'trip_count': len(dropoff_df),
                    'avg_fare': round(float(dropoff_df['fare_amount'].mean()), 2) if len(dropoff_df) > 0 else 0,
                    'avg_distance': round(float(dropoff_df['trip_distance'].mean()), 2) if len(dropoff_df) > 0 else 0,
                    'top_origins': dropoff_df['PU_Zone'].value_counts().head(5).to_dict() if len(dropoff_df) > 0 else {}
                }
            
            if analysis_type == 'both':
                results[tt] = {
                    'borough': borough_title,
                    'pickups': pickup_stats,
                    'dropoffs': dropoff_stats
                }
            elif analysis_type == 'pickup':
                results[tt] = {'borough': borough_title, 'pickups': pickup_stats}
            else:
                results[tt] = {'borough': borough_title, 'dropoffs': dropoff_stats}
        else:
            # Summary for all boroughs
            pickup_by_borough = df.groupby('PU_Borough').agg({
                'fare_amount': ['count', 'mean'],
                'trip_distance': 'mean'
            }).round(2)
            
            results[tt] = {
                'all_boroughs': {
                    borough: {
                        'pickup_count': int(pickup_by_borough.loc[borough, ('fare_amount', 'count')]),
                        'avg_fare': float(pickup_by_borough.loc[borough, ('fare_amount', 'mean')]),
                        'avg_distance': float(pickup_by_borough.loc[borough, ('trip_distance', 'mean')])
                    }
                    for borough in pickup_by_borough.index
                }
            }
    
    return json.dumps(results, indent=2)

def get_routes_by_criteria(
    df_green: pd.DataFrame,
    df_yellow: pd.DataFrame,
    taxi_type: Literal['green', 'yellow', 'both'] = 'both',
    min_trips: int = 10,
    min_fare: Optional[float] = None,
    max_fare: Optional[float] = None,
    min_distance: Optional[float] = None,
    max_distance: Optional[float] = None,
    top_n: int = 10
) -> str:
    results = {}
    
    for tt in _get_taxi_types(taxi_type):
        df = get_df(df_green, df_yellow, tt)
        
        # Group by routes
        routes = df.groupby(['PU_Zone', 'DO_Zone']).agg({
            'fare_amount': ['count', 'mean'],
            'trip_distance': 'mean'
        }).round(2)
        
        routes.columns = ['trip_count', 'avg_fare', 'avg_distance']
        
        # Apply filters
        routes = routes[routes['trip_count'] >= min_trips]
        
        if min_fare is not None:
            routes = routes[routes['avg_fare'] >= min_fare]
        if max_fare is not None:
            routes = routes[routes['avg_fare'] <= max_fare]
        if min_distance is not None:
            routes = routes[routes['avg_distance'] >= min_distance]
        if max_distance is not None:
            routes = routes[routes['avg_distance'] <= max_distance]
        
        if len(routes) == 0:
            results[tt] = {'error': 'No routes matching the specified criteria'}
            continue
        
        # Sort by trip count and get top N
        routes = routes.sort_values('trip_count', ascending=False).head(top_n)
        
        results[tt] = {
            'filters_applied': {
                'min_trips': min_trips,
                'min_fare': min_fare,
                'max_fare': max_fare,
                'min_distance': min_distance,
                'max_distance': max_distance
            },
            'routes_found': len(routes),
            'routes': [
                {
                    'rank': i + 1,
                    'pickup_zone': pu,
                    'dropoff_zone': do,
                    'trip_count': int(row['trip_count']),
                    'avg_fare': float(row['avg_fare']),
                    'avg_distance': float(row['avg_distance'])
                }
                for i, ((pu, do), row) in enumerate(routes.iterrows())
            ]
        }
    
    return json.dumps(results, indent=2)
