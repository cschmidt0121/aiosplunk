import logging
from asyncio import Queue, create_task, sleep
from enum import Enum
from json import loads
from typing import Any

from .client import Client

logger = logging.getLogger(__name__)


class OutputMode(Enum):
    csv = "csv"
    dict = "dict"
    raw = "raw"


class Search:
    def __init__(
        self,
        splunk_client: Client,
        search_string: str,
        earliest: str,
        latest: str,
        output_mode: OutputMode = OutputMode.dict,
        num_workers: int = 25,
        # 10k is usually the best if you want the data ASAP but will block on
        # parsing more. Smaller chunk size = faster coroutines
        chunk_size: int = 10000,
    ):
        self.splunk_client = splunk_client
        self.search_string = search_string
        self.earliest = earliest
        self.latest = latest

        try:
            OutputMode(output_mode)
        except ValueError:
            raise ValueError(
                f"Invalid output_mode: {output_mode}. Valid values are csv, dict, and raw."
            )

        self.output_mode = output_mode
        self.num_workers = num_workers
        self.chunk_size = chunk_size

        self.sid = None
        self.job_summary = {}
        self.to_be_retrieved = Queue()
        self.output_chunks = Queue()

    @classmethod
    async def from_sid(
        cls,
        splunk_client: Client,
        sid: str,
        output_mode: OutputMode = OutputMode.dict,
        num_workers: int = 25,
        chunk_size: int = 10000,
    ):
        search = cls(
            splunk_client=splunk_client,
            search_string="",
            earliest="",
            latest="",
            output_mode=output_mode,
            num_workers=num_workers,
            chunk_size=chunk_size
        )
        search.sid = sid
        search.job_summary = await splunk_client.get_summary(sid)
        return search

    async def run(self):
        """
        Run the search job
        """
        job_def = {
            "search": self.search_string,
            "earliest_time": self.earliest,
            "latest_time": self.latest,
            "timeout": 180,
        }

        self.sid = await self.splunk_client.run_search(**job_def)
        self.job_summary = await self.wait_and_get_summary()


    async def wait_and_get_summary(self):
        """
        Wait until the job is complete, then return the summary
        """
        if not self.sid:
            raise ValueError("Can't get results with no SID")

        running_states = ("QUEUED", "PARSING", "RUNNING", "FINALIZING", "PAUSE")

        while True:
            job_summary = await self.splunk_client.get_job(self.sid)

            state = job_summary["dispatchState"]

            if state not in running_states:
                return job_summary
            await sleep(0.5)

    async def get_chunks(self, fields: list | None = None):
        """
        Split results into <self.chunk_size> chunks, and then assign 
        <self.num_workers> workers to grab them concurrently. 
        """
        if not self.sid:
            raise Exception("Cannot get results without an SID.")

        result_count = self.job_summary.get("resultCount", 500000)

        # Each queue item is for a chunk of <self.chunk_size> results
        for offset in range(0, result_count, self.chunk_size):
            await self.to_be_retrieved.put((offset))

        qsize = self.to_be_retrieved.qsize()

        logger.debug("Creating workers")
        workers = [
            create_task(self.result_worker(fields=fields))
            for n in range(self.num_workers)
        ]

        # Run until we have processed <qsize> chunks
        processed = 0
        while True:
            chunk = await self.output_chunks.get()
            yield chunk

            processed += 1
            if processed == qsize:
                break

        logger.debug("All chunks returned")

        for w in workers:
            w.cancel()

    def parse_chunk(self, chunk_data: str, offset: int) -> list[Any]:
        """
        Given a chunk of results, process it and turn it into a list.
        If output_mode is raw/csv, it will be a list of strings. If output_mode is
        dict, it will be a list of dicts.
        """
        if self.output_mode == OutputMode.raw:
            return chunk_data.splitlines()

        elif self.output_mode == OutputMode.csv:
            lines = chunk_data.splitlines()

            # Avoid repeated header
            if offset != 0:
                lines = lines[1:]
            return lines

        elif self.output_mode == OutputMode.dict:
            parsed = loads(chunk_data)
            return parsed["results"]
        
        else:
            raise ValueError(f"Unknown output_mode: {self.output_mode}")

    async def get_results(self, fields: list | None = None):
        print("get chunks")
        chunks = [item async for item in self.get_chunks(fields=fields)] 

        print("sort chunks")
        chunks_sorted = await self.sort_chunks(chunks)

        # Offset is the first item in each chunk tuple. For CSV parsing it needs to
        # be passed along so that the header can be printed out exactly once.
        for offset, chunk in chunks_sorted:
            print("parse chunk")
            await sleep(0)  # Yield control back to the event loop
            for row in self.parse_chunk(
                chunk_data=chunk, offset=offset
            ):
                yield row

    @staticmethod
    async def sort_chunks(chunks: list):
        chunks = sorted(chunks, key=lambda chunk: chunk[0])

        return chunks



    async def result_worker(self, fields: list | None = None) -> None:
        if not self.sid:
            raise ValueError("Can't get results with no SID")
        
        while True:
            offset = await self.to_be_retrieved.get()

            output_mode_str = (
                "json" if self.output_mode == OutputMode.dict else self.output_mode.value
            )
            results = await self.splunk_client.get_results(
                self.sid, self.chunk_size, offset, output_mode_str, fields
            )

            chunk = (offset, results)

            await self.output_chunks.put(chunk)
            self.to_be_retrieved.task_done()
