# create_es_index.ps1
# Creates the Elasticsearch index with proper mapping for STREAMSIGHT
#
# Run: .\create_es_index.ps1

$ES_HOST = "http://localhost:9200"
$INDEX_NAME = "streamsight-trades"

# Delete existing index (if any)
Write-Host "Deleting existing index (if any)..."
try {
    Invoke-RestMethod -Uri "$ES_HOST/$INDEX_NAME" -Method Delete -ErrorAction SilentlyContinue
} catch {
    Write-Host "Index did not exist, continuing..."
}

# Create index with mapping
$mapping = @'
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "5s"
  },
  "mappings": {
    "properties": {
      "symbol": {
        "type": "keyword"
      },
      "window_start": {
        "type": "date"
      },
      "window_end": {
        "type": "date"
      },
      "total_volume": {
        "type": "double"
      },
      "trade_count": {
        "type": "long"
      },
      "avg_price": {
        "type": "double"
      },
      "vwap": {
        "type": "double"
      },
      "price_stddev": {
        "type": "double"
      },
      "zscore": {
        "type": "double"
      },
      "is_anomaly": {
        "type": "boolean"
      },
      "anomaly_type": {
        "type": "keyword"
      },
      "doc_id": {
        "type": "keyword"
      }
    }
  }
}
'@

Write-Host "Creating index with mapping..."
$response = Invoke-RestMethod -Uri "$ES_HOST/$INDEX_NAME" -Method Put -Body $mapping -ContentType "application/json"
Write-Host "Index created successfully!"
Write-Host $response | ConvertTo-Json
