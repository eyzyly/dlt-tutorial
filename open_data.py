import dlt

from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

# This is a generic pipeline example and demonstrates
# how to use the dlt REST client for extracting data from APIs.
# It showcases the use of authentication via bearer tokens and pagination.


@dlt.source
def open_data_source(api_secret_key: str = dlt.secrets.value):
    # print(f"api_secret_key={api_secret_key}")
    return open_data_resource(api_secret_key)


@dlt.resource(write_disposition="append")
def open_data_resource(
    api_secret_key: str = dlt.secrets.value,
    org: str = "dlt-hub",
    repository: str = "dlt",
):
    # this is the test data for loading validation, delete it once you yield actual data
    yield [
        {
            "id": 1,
            "node_id": "MDU6SXNzdWUx",
            "number": 1347,
            "state": "open",
            "title": "Found a bug",
            "body": "I'm having a problem with this.",
            "user": {"login": "octocat", "id": 1},
            "created_at": "2011-04-22T13:33:48Z",
            "updated_at": "2011-04-22T13:33:48Z",
            "repository": {
                "id": 1296269,
                "node_id": "MDEwOlJlcG9zaXRvcnkxMjk2MjY5",
                "name": "Hello-World",
                "full_name": "octocat/Hello-World",
            },
        }
    ]

    # paginate issues and yield every page
    # api_url = f"https://api.github.com/repos/{org}/{repository}/issues"
    # for page in paginate(
    #     api_url,
    #     auth=BearerTokenAuth(api_secret_key),
    #     # Note: for more paginators please see:
    #     # https://dlthub.com/devel/general-usage/http/rest-client#paginators
    #     paginator=HeaderLinkPaginator(),
    # ):
    #     # print(page)
    #     yield page


if __name__ == "__main__":
    # specify the pipeline name, destination and dataset name when configuring pipeline,
    # otherwise the defaults will be used that are derived from the current script name
    pipeline = dlt.pipeline(
        pipeline_name='open_data',
        destination='bigquery',
        dataset_name='open_data_data',
    )

    data = list(open_data_resource())

    # print the data yielded from resource
    print(data)

    # run the pipeline with your parameters
    load_info = pipeline.run(open_data_resource())

    # pretty print the information on data that was loaded
    print(load_info)
