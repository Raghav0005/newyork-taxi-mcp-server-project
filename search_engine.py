import json
import pandas as pd
from pathlib import Path
from whoosh import index
from whoosh.fields import Schema, TEXT, NUMERIC, ID, KEYWORD
from whoosh.qparser import MultifieldParser
from whoosh.query import And, Term, Every
from whoosh.analysis import StemmingAnalyzer

class TaxiSearchEngine:
    def __init__(self, index_dir: str = 'search_index'):
        self.index_dir = Path(index_dir)
        self.index_dir.mkdir(exist_ok=True)
        
        # defined schema for taxi trip data
        self.schema = Schema(
            trip_id=ID(stored=True, unique=True),
            taxi_type=KEYWORD(stored=True),
            pickup_zone=TEXT(stored=True, analyzer=StemmingAnalyzer()),
            dropoff_zone=TEXT(stored=True, analyzer=StemmingAnalyzer()),
            pickup_borough=KEYWORD(stored=True),
            dropoff_borough=KEYWORD(stored=True),
            fare_amount=NUMERIC(stored=True, numtype=float),
            trip_distance=NUMERIC(stored=True, numtype=float),
            hour=NUMERIC(stored=True, numtype=int),
            day_of_week=KEYWORD(stored=True),
            period=KEYWORD(stored=True),  # Peak or Off-Peak
            date=TEXT(stored=True),
            # Combined text field for full-text search
            content=TEXT(analyzer=StemmingAnalyzer())
        )
        
        self.ix = None
    
    def create_index(self, df_green: pd.DataFrame, df_yellow: pd.DataFrame, force_rebuild: bool = False):
        if index.exists_in(str(self.index_dir)) and not force_rebuild:
            self.ix = index.open_dir(str(self.index_dir))
            return
        
        self.ix = index.create_in(str(self.index_dir), self.schema)
        writer = self.ix.writer()
        
        self._index_dataframe(writer, df_green, 'green')
        self._index_dataframe(writer, df_yellow, 'yellow')
        
        writer.commit()
    
    def _index_dataframe(self, writer, df: pd.DataFrame, taxi_type: str):
        df_sample = df.sample(n=min(500000, len(df)), random_state=42)
        
        for idx, row in df_sample.iterrows():
            content = ' '.join(filter(None, [
                str(row.get('PU_Zone', '')),
                str(row.get('DO_Zone', '')),
                str(row.get('PU_Borough', '')),
                str(row.get('DO_Borough', '')),
                str(row.get('day_of_week', '')),
                str(row.get('period', ''))
            ]))
            
            writer.add_document(
                trip_id=f"{taxi_type}_{idx}",
                taxi_type=taxi_type,
                pickup_zone=str(row.get('PU_Zone', 'Unknown')),
                dropoff_zone=str(row.get('DO_Zone', 'Unknown')),
                pickup_borough=str(row.get('PU_Borough', 'Unknown')),
                dropoff_borough=str(row.get('DO_Borough', 'Unknown')),
                fare_amount=float(row.get('fare_amount', 0.0)),
                trip_distance=float(row.get('trip_distance', 0.0)),
                hour=int(row.get('hour', 0)),
                day_of_week=str(row.get('day_of_week', 'Unknown')),
                period=str(row.get('period', 'Unknown')),
                date=str(row.get('date', '')),
                content=content
            )
    
    def _format_hit(self, hit) -> dict:
        return {
            'trip_id': hit['trip_id'],
            'taxi_type': hit['taxi_type'],
            'pickup_zone': hit['pickup_zone'],
            'dropoff_zone': hit['dropoff_zone'],
            'pickup_borough': hit['pickup_borough'],
            'dropoff_borough': hit['dropoff_borough'],
            'fare_amount': hit['fare_amount'],
            'trip_distance': hit['trip_distance'],
            'hour': hit['hour'],
            'day_of_week': hit['day_of_week'],
            'period': hit['period'],
            'date': hit['date'],
            'score': hit.score if hasattr(hit, 'score') else 1.0
        }
    
    def open_index(self):
        if not index.exists_in(str(self.index_dir)):
            raise ValueError(f"No index found at {self.index_dir}. Create one first.")
        self.ix = index.open_dir(str(self.index_dir))
    
    def search(self, query_string: str, limit: int = 20, search_type: str = 'all') -> dict:
        """Search the index with query string."""
        if self.ix is None:
            self.open_index()
        
        fields = {
            'zones': ['pickup_zone', 'dropoff_zone'],
            'content': ['content']
        }.get(search_type, ['pickup_zone', 'dropoff_zone', 'pickup_borough', 'dropoff_borough', 'content'])
        
        with self.ix.searcher() as searcher:
            parser = MultifieldParser(fields, schema=self.schema)
            results = searcher.search(parser.parse(query_string), limit=limit)
            
            hits = [self._format_hit(hit) for hit in results]
            
            return {
                'query': query_string,
                'total_results': len(results),
                'showing': min(limit, len(results)),
                'results': hits
            }
    
    def search_with_filters(self, 
                           query_string: str = None,
                           taxi_type: str = None,
                           pickup_borough: str = None,
                           dropoff_borough: str = None,
                           min_fare: float = None,
                           max_fare: float = None,
                           period: str = None,
                           day_of_week: str = None,
                           limit: int = 20) -> dict:
        """Advanced search with multiple filters."""
        if self.ix is None:
            self.open_index()
        
        with self.ix.searcher() as searcher:
            query_parts = []
            
            if query_string:
                parser = MultifieldParser(['pickup_zone', 'dropoff_zone', 'content'], schema=self.schema)
                query_parts.append(parser.parse(query_string))
            
            for field, value in [('taxi_type', taxi_type), ('pickup_borough', pickup_borough),
                                ('dropoff_borough', dropoff_borough), ('period', period), 
                                ('day_of_week', day_of_week)]:
                if value:
                    query_parts.append(Term(field, value))
            
            final_query = And(query_parts) if query_parts else Every()
            results = searcher.search(final_query, limit=limit)
            
            hits = [
                self._format_hit(hit) for hit in results
                if (min_fare is None or hit['fare_amount'] >= min_fare) and
                   (max_fare is None or hit['fare_amount'] <= max_fare)
            ]
            
            return {
                'query': query_string or 'filtered search',
                'filters': {
                    'taxi_type': taxi_type,
                    'pickup_borough': pickup_borough,
                    'dropoff_borough': dropoff_borough,
                    'min_fare': min_fare,
                    'max_fare': max_fare,
                    'period': period,
                    'day_of_week': day_of_week
                },
                'total_results': len(hits),
                'showing': min(limit, len(hits)),
                'results': hits[:limit]
            }
    
    def get_doc_count(self) -> int:
        if self.ix is None:
            self.open_index()
        with self.ix.searcher() as searcher:
            return searcher.doc_count_all()
    
    def get_index_stats(self) -> dict:
        if self.ix is None:
            self.open_index()
        
        with self.ix.searcher() as searcher:
            # Get taxi type distribution
            taxi_types = {}
            for taxi_type in ['green', 'yellow']:
                query = Term('taxi_type', taxi_type)
                results = searcher.search(query, limit=None)
                taxi_types[taxi_type] = len(results)
            
            return {
                'total_documents': searcher.doc_count_all(),
                'index_location': str(self.index_dir),
                'taxi_type_distribution': taxi_types,
                'schema_fields': list(self.schema.names())
            }


# Convenience function for quick searches
def search_taxi_data(query: str, limit: int = 20) -> str:
    engine = TaxiSearchEngine()
    results = engine.search(query, limit=limit)
    return json.dumps(results, indent=2)
