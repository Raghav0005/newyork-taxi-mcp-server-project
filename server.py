import json
import os
from pathlib import Path
from typing import Optional
from enum import Enum
from mcp.server.fastmcp import FastMCP
from data_loader import TaxiDataLoader
from search_engine import TaxiSearchEngine
import tools

BASE_DIR = Path(__file__).resolve().parent
os.chdir(BASE_DIR)

class TaxiType(str, Enum):
    GREEN = "green"
    YELLOW = "yellow"
    BOTH = "both"

class Period(str, Enum):
    PEAK = "Peak"
    OFF_PEAK = "Off-Peak"

class TemporalMetric(str, Enum):
    BY_HOUR = "by_hour"
    BY_DAY = "by_day"
    PEAK_VS_OFFPEAK = "peak_vs_offpeak"

class LocationAnalysis(str, Enum):
    TOP_PICKUPS = "top_pickups"
    TOP_DROPOFFS = "top_dropoffs"
    BY_BOROUGH = "by_borough"
    BY_TIME = "by_time"

class RouteAnalysis(str, Enum):
    POPULAR = "popular"
    BY_CRITERIA = "by_criteria"

class FareAnalysis(str, Enum):
    STATISTICS = "statistics"
    COMPARE_TYPES = "compare_types"

mcp = FastMCP("NYC Taxi Analytics")

df_green = None
df_yellow = None
zone_lookup = None
search_engine = None

def load_data():
    global df_green, df_yellow, zone_lookup, search_engine
    loader = TaxiDataLoader(data_dir='data')
    df_green, df_yellow, zone_lookup = loader.load_all_data()
    search_engine = TaxiSearchEngine(index_dir='search_index')
    search_engine.create_index(df_green, df_yellow, force_rebuild=False)

@mcp.tool()
def query_trips(
    query_text: Optional[str] = None,
    taxi_type: TaxiType = TaxiType.BOTH,
    pickup_location: Optional[str] = None,
    dropoff_location: Optional[str] = None,
    min_fare: Optional[float] = None,
    max_fare: Optional[float] = None,
    min_distance: Optional[float] = None,
    max_distance: Optional[float] = None,
    day_of_week: Optional[str] = None,
    hour: Optional[int] = None,
    period: Optional[Period] = None,
    limit: int = 20
) -> str:
    # Deterministic routing: Use search for text-based relevance, pandas for numeric precision
    has_text_query = bool(query_text)
    has_location_text = bool(pickup_location or dropoff_location)
    has_numeric_filters = bool(min_distance is not None or max_distance is not None)
    
    # Decision: Search engine for text relevance, pandas for numeric filtering
    use_search = has_text_query or (has_location_text and not has_numeric_filters)
    
    if use_search:
        # SEARCH ENGINE PATH: Find relevant examples using full-text search
        taxi_filter = None if taxi_type == TaxiType.BOTH else taxi_type.value
        period_filter = period.value if period else None
        
        # Build query string from all text inputs
        query_parts = [query_text] if query_text else []
        if pickup_location:
            query_parts.append(pickup_location)
        if dropoff_location:
            query_parts.append(dropoff_location)
        
        return json.dumps(search_engine.search_with_filters(
            query_string=' '.join(query_parts),
            taxi_type=taxi_filter,
            min_fare=min_fare,
            max_fare=max_fare,
            period=period_filter,
            day_of_week=day_of_week,
            limit=limit
        ), indent=2)
    else:
        # PANDAS PATH: Precise filtering and statistics on full dataset
        period_filter = period.value if period else None
        return tools.search_trips(
            df_green, df_yellow, taxi_type.value,
            min_fare, max_fare, min_distance, max_distance,
            pickup_location, dropoff_location,
            day_of_week, hour, period_filter
        )

@mcp.tool()
def analyze_temporal(
    metric: TemporalMetric = TemporalMetric.BY_HOUR,
    taxi_type: TaxiType = TaxiType.BOTH,
    specific_hour: Optional[int] = None,
    specific_day: Optional[str] = None
) -> str:
    if metric == TemporalMetric.BY_HOUR:
        return tools.get_trip_volume_by_hour(df_green, df_yellow, taxi_type.value, specific_hour)
    elif metric == TemporalMetric.BY_DAY:
        return tools.get_trip_volume_by_day(df_green, df_yellow, taxi_type.value, specific_day)
    else:
        return tools.get_peak_vs_offpeak_stats(df_green, df_yellow, taxi_type.value)

@mcp.tool()
def analyze_locations(
    analysis_type: LocationAnalysis = LocationAnalysis.TOP_PICKUPS,
    taxi_type: TaxiType = TaxiType.BOTH,
    borough: Optional[str] = None,
    day_of_week: Optional[str] = None,
    hour: Optional[int] = None,
    period: Optional[Period] = None,
    top_n: int = 10
) -> str:
    period_value = period.value if period else None
    
    if analysis_type == LocationAnalysis.TOP_PICKUPS:
        return tools.get_top_pickup_zones(df_green, df_yellow, taxi_type.value, top_n)
    elif analysis_type == LocationAnalysis.TOP_DROPOFFS:
        return tools.get_top_dropoff_zones(df_green, df_yellow, taxi_type.value, top_n)
    elif analysis_type == LocationAnalysis.BY_BOROUGH:
        return tools.get_borough_analysis(df_green, df_yellow, taxi_type.value, borough, 'both')
    else:
        return tools.get_zones_by_time(
            df_green, df_yellow, taxi_type.value, 'pickup',
            day_of_week, hour, period_value, top_n
        )

@mcp.tool()
def analyze_routes(
    analysis_type: RouteAnalysis = RouteAnalysis.POPULAR,
    taxi_type: TaxiType = TaxiType.BOTH,
    min_trips: int = 10,
    min_fare: Optional[float] = None,
    max_fare: Optional[float] = None,
    min_distance: Optional[float] = None,
    max_distance: Optional[float] = None,
    top_n: int = 10
) -> str:
    if analysis_type == RouteAnalysis.POPULAR:
        return tools.get_popular_routes(df_green, df_yellow, taxi_type.value, top_n)
    else:
        return tools.get_routes_by_criteria(
            df_green, df_yellow, taxi_type.value,
            min_trips, min_fare, max_fare, min_distance, max_distance, top_n
        )

@mcp.tool()
def analyze_fares(
    analysis_type: FareAnalysis = FareAnalysis.STATISTICS,
    taxi_type: TaxiType = TaxiType.BOTH,
    period: Optional[Period] = None,
    hour: Optional[int] = None
) -> str:
    period_value = period.value if period else None
    
    if analysis_type == FareAnalysis.STATISTICS:
        return tools.get_fare_statistics(df_green, df_yellow, taxi_type.value, period_value, hour)
    else:
        results = {}
        for metric in ['trip_volume', 'avg_fare', 'avg_distance', 'peak_distribution']:
            comparison = json.loads(tools.compare_taxi_types(df_green, df_yellow, metric))
            results[metric] = comparison
        return json.dumps(results, indent=2)

@mcp.tool()
def get_dataset_info(include_search_stats: bool = True) -> str:
    summary = json.loads(tools.get_dataset_summary(df_green, df_yellow, zone_lookup))
    if include_search_stats:
        summary['search_index'] = search_engine.get_index_stats()
    
    return json.dumps(summary, indent=2)

if __name__ == "__main__":
    load_data()
    mcp.run()
