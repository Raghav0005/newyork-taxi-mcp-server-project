# NYC Taxi MCP Server

## Dataset
- **Green taxi**: 48,326 trips
- **Yellow taxi**: 3,475,226 trips  
- **Period**: January 2025
- **Source**: NYC TLC Trip Record Data

## Data
```
server.py          # MCP server (Enum-based API)
├── search_engine.py   # Whoosh full-text search (548k indexed)
├── tools.py           # Pandas analytics (full 3.5M dataset)
└── data_loader.py     # Data loading + enrichment (used ideas learned from part (2))
```

`data-insights.ipynb` stores the data munging, cleaning, and visualizations

## MCP Tools

1. **query_trips** - Search/filter trips (auto-routes to Whoosh or Pandas, as needed)
2. **analyze_temporal** - Hourly, daily, peak vs off-peak patterns
3. **analyze_locations** - Top zones, borough analysis, time-based locations
4. **analyze_routes** - Popular routes, filtered by criteria
5. **analyze_fares** - Statistics, taxi type comparison
6. **get_dataset_info** - Dataset summary + search index stats

**Routing Logic:**
- Text queries (locations, natural language) → Whoosh (relevance-ranked examples)
- Numeric filters (distance, precise ranges) → Pandas (full dataset statistics)

## Setup

```bash
pip install pandas pyarrow fastmcp whoosh
```

I connected this with the Gemini CLI.
This required giving the path to the python being used in the environment, as well as the path to the MCP server.

After installing the Gemini CLI with Node, update the corresponding `~/.gemini/settings.json` file with:

```
"mcpServers": {
    "nyc-taxi": {
      "command": "/.../envs/cs451/bin/python",
      "args": ["/.../Desktop/CS451/project/server.py"]
    }
  }
```
