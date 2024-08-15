import dlt
from dlt.sources.helpers.rest_client import paginate

BASE_GITHUB_URL = "https://api.github.com/repos/dlt-hub/dlt"

def fetch_github_data(endpoint, params={}):
    url = f"{BASE_GITHUB_URL}/{endpoint}"
    return paginate(url, params=params)

@dlt.source
def github_source():
    for endpoint in ["issues", "comments"]:
        params = {"per_page": 100}
        yield dlt.resource(
            fetch_github_data(endpoint, params),
            name=endpoint,
            write_disposition="merge",
            primary_key="id",
        )

pipeline = dlt.pipeline(
    pipeline_name='github_dynamic_source',
    destination='duckdb',
    dataset_name='github_data',
)
load_info = pipeline.run(github_source())
row_counts = pipeline.last_trace.last_normalize_info