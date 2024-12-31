from google.cloud import bigquery

EVENT_INSIGHTS_SCHEMA = [
    bigquery.SchemaField("total_events", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("events_by_day", "RECORD", mode="REPEATED",
        fields=[
            bigquery.SchemaField("day", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("count", "INTEGER", mode="REQUIRED")
        ]
    ),
    bigquery.SchemaField("events_by_hour", "RECORD", mode="REPEATED",
        fields=[
            bigquery.SchemaField("hour", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("count", "INTEGER", mode="REQUIRED")
        ]
    ),
    bigquery.SchemaField("weekend_vs_weekday", "RECORD", mode="REQUIRED",
        fields=[
            bigquery.SchemaField("weekend", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("weekday", "INTEGER", mode="REQUIRED")
        ]
    ),
    bigquery.SchemaField("price_distribution", "RECORD", mode="REQUIRED",
        fields=[
            bigquery.SchemaField("free", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("paid", "INTEGER", mode="REQUIRED")
        ]
    ),
    bigquery.SchemaField("popular_categories", "RECORD", mode="REPEATED",
        fields=[
            bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("count", "INTEGER", mode="REQUIRED")
        ]
    ),
    bigquery.SchemaField("popular_venues", "RECORD", mode="REPEATED",
        fields=[
            bigquery.SchemaField("venue", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("count", "INTEGER", mode="REQUIRED")
        ]
    ),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED")
]

TRENDS_INSIGHTS_SCHEMA = [
    bigquery.SchemaField("total_topics", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("top_topics_by_interest", "RECORD", mode="REPEATED",
        fields=[
            bigquery.SchemaField("topic", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("interest_value", "FLOAT", mode="REQUIRED")
        ]
    ),
    bigquery.SchemaField("interest_distribution", "RECORD", mode="REQUIRED",
        fields=[
            bigquery.SchemaField("high", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("medium", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("low", "INTEGER", mode="REQUIRED")
        ]
    ),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED")
]

def create_insights_tables(project_id: str, dataset_id: str):
    """Create BigQuery tables for insights if they don't exist."""
    client = bigquery.Client()
    dataset_ref = f"{project_id}.{dataset_id}"
    
    # Create event insights table
    event_table = bigquery.Table(
        f"{dataset_ref}.event_insights",
        schema=EVENT_INSIGHTS_SCHEMA
    )
    event_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="timestamp"
    )
    try:
        client.create_table(event_table)
        print(f"Created table {event_table.table_id}")
    except Exception as e:
        if "Already Exists" not in str(e):
            raise e
    
    # Create trends insights table
    trends_table = bigquery.Table(
        f"{dataset_ref}.trends_insights",
        schema=TRENDS_INSIGHTS_SCHEMA
    )
    trends_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="timestamp"
    )
    try:
        client.create_table(trends_table)
        print(f"Created table {trends_table.table_id}")
    except Exception as e:
        if "Already Exists" not in str(e):
            raise e 