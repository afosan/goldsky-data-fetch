from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport


def get_client(url: str):
    transport = AIOHTTPTransport(url=url)
    client = Client(
        transport=transport,
        fetch_schema_from_transport=True,
    )

    return client


async def query_until_end(
    client,
    query,
):
    skip = 0
    first = 1000

    all_data = []

    while True:
        result = await client.execute_async(
            query,
            variable_values={
                "skip": skip,
                "first": first,
            },
        )

        key = list(result.keys())[0]
        data = result[key]
        count = len(data)
        all_data.extend(data)

        if count < first:
            break
        else:
            skip += first

    return {key: all_data}
