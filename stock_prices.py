import datetime
import requests
from ast import literal_eval
from dotenv import load_dotenv
from aws_secret_manager import get_secret

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, DateType, FloatType, LongType
from pyiceberg.partitioning import PartitionSpec

load_dotenv()


def homework_script():
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']

    polygon_api_key = literal_eval(get_secret("POLYGON_CREDENTIALS"))['AWS_SECRET_ACCESS_KEY']
    catalog = load_catalog(
        'academy',
        type="rest",
        uri="https://api.tabular.io/ws",
        warehouse=get_secret("CATALOG_NAME"),
        credential=get_secret("TABULAR_CREDENTIAL")
    )

    identifier = 'bootcamp.sirdonaldo_maang_stock_prices'

    iceberg_schema = Schema(
        Schema.field("symbol", StringType()),
        Schema.field("date", DateType()),
        Schema.field("open", FloatType()),
        Schema.field("high", FloatType()),
        Schema.field("low", FloatType()),
        Schema.field("close", FloatType()),
        Schema.field("volume", LongType())
    )

    partition_spec = PartitionSpec().identity("date")

    if not catalog.table_exists(identifier):
        catalog.create_table(
            identifier=identifier,
            schema=iceberg_schema,
            partition_spec=partition_spec
        )
        print("Iceberg table created")

    table = catalog.load_table(identifier)

    try:
        table.create_branch("dev")
        print("Branch 'dev' created")
    except Exception as e:
        print(f" Branch 'dev' may already exist: {e}")

    for ticker in maang_stocks:
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2025-01-05/2025-01-06?adjusted=true&sort=asc&apiKey={polygon_api_key}"
        response = requests.get(url)
        data = response.json()

        if 'results' not in data:
            print(f" No results for {ticker}")
            continue

        rows = []
        for result in data['results']:
            timestamp = result.get('t')
            date = datetime.datetime.utcfromtimestamp(timestamp / 1000).date()

            rows.append({
                'symbol': ticker,
                'date': date,
                'open': result.get('o', 0.0),
                'high': result.get('h', 0.0),
                'low': result.get('l', 0.0),
                'close': result.get('c', 0.0),
                'volume': result.get('v', 0)
            })

        try:
            table.append_rows(rows, branch="dev")
            print(f" Ingested {len(rows)} rows for {ticker}")
        except Exception as e:
            print(f" Failed to ingest rows for {ticker}: {e}")


if __name__ == "__main__":
    homework_script()
