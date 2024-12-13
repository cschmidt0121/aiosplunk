# aiosplunk

An asyncio-based Python module for interacting with Splunk. Currently limited to only searches.
Very much a work-in-progress.

## Examples

### Run a search and dump results out to a CSV file

```
import aiofiles

from aiosplunk import Client, Search
from aiosplunk.search import OutputMode

client = Client(host="localhost", username="user", password="pass")

search = Search(
    splunk_client=client,
    search_string="search index=_internal | head 500000",
    earliest="-4h",
    latest="now",
    output_mode=OutputMode.csv,
)

await search.run()f
async with aiofiles.open("out.csv", "w") as f:
    lines = []
    async for line in s.get_results(fields=["_raw", "host"]):
        lines.append(str(line) + "\n")

    await f.writelines(lines)
```

### Pull down results from an existing search and do something with the results

```
from aiosplunk import Client, Search
from aiosplunk.search import OutputMode

client = Client(host="localhost", username="user", password="pass")

search = await Search.from_sid(
    splunk_client=client, sid="1734103773.893674", output_mode=OutputMode.dict
)

lines = []
async for d in s.get_results(fields=["_raw", "host", "my_field"]):
    my_field = d.get("my_field")
    ...

```