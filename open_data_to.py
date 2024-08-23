import requests
import dlt
import pandas as pd
from io import StringIO
import random
 

@dlt.source
def open_data_source():
    return extract_open_data()


@dlt.resource(    
        table_name="extract_open_data",
        write_disposition="replace")
def extract_open_data():
     
    # Toronto Open Data is stored in a CKAN instance. It's APIs are documented here:
    # https://docs.ckan.org/en/latest/api/
    
    # To hit our API, you'll be making requests to:
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    
    # Datasets are called "packages". Each package can contain many "resources"
    # To retrieve the metadata for this package and its resources, use the package name in this page's URL:
    url = base_url + "/api/3/action/package_show"
    params = { "id": "short-term-rentals-registration"}
    package = requests.get(url, params = params).json()
    
    # To get resource data:
    for idx, resource in enumerate(package["result"]["resources"]):
    
        # for datastore_active resources:
        if resource["datastore_active"]:
    
            # To get all records in CSV format:
            url = base_url + "/datastore/dump/" + resource["id"]
            resource_dump_data = requests.get(url).text
            df = pd.read_csv(StringIO(resource_dump_data))
            json_df = df.to_dict('records')
            yield json_df

if __name__ == "__main__":
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    pipeline = dlt.pipeline(
        pipeline_name='open_data_short_term_rental',
        destination='bigquery',
        dataset_name='open_data_short_term_rental',
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(extract_open_data)
    row_counts = pipeline.last_trace.last_normalize_info

    print(row_counts)
    print("------")
    print(load_info)

