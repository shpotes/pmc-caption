import asyncio
import pathlib
from typing import Optional

import aioftp
import pandas as pd
import uvloop


class Downloader:
    def __init__(self, data_dir, metadata, num_samples=None):
        self.data_dir = pathlib.Path(data_dir)
        self.target_dir = self.data_dir / 'raw'
        self.target_dir.mkdir(exist_ok=True, parents=True)

        self.metadata = self._prepare_metadata(
            metadata,
            num_samples
        )

        self._host = 'ftp.ncbi.nlm.nih.gov'
        self._promp = 'pub/pmc/oa_package'

    def update_metadata(self, meta, num_samples):
        first_sample = len(list(
            filter(lambda x: x.is_file(), self.target_dir.glob('**/*.tar.gz'))
        ))
        meta = meta.iloc[first_sample:]

        if num_samples:
            last_sample = first_sample + num_samples
            meta = meta.iloc[:last_sample]

        return meta, first_sample

    def _prepare_metadata(
            self,
            meta: pd.DataFrame,
            num_samples: Optional[int] = None
    ) -> pd.DataFrame:
        """
        preprocess pmc metadata file
        """

        meta, _ = self.update_metadata(meta, num_samples)

        if meta.empty:
            raise NotImplementedError

        return meta

    async def schedule(self, semaphore, job_queue):
        while not job_queue.empty():
            queue_url = await job_queue.get()
            async with semaphore:
                async with aioftp.Client.context(self._host, socket_timeout=120) as sess:
                    await sess.download(
                        f'{self._promp}/{queue_url}',
                        self.target_dir / queue_url,
                        block_size=33554432,
                    )

    def download(self):
        loop = uvloop.new_event_loop()
        asyncio.set_event_loop(loop)

        queue = asyncio.Queue()
        semaphore = asyncio.Semaphore(10)

        _ = [queue.put_nowait(url) for url in self.metadata.fname]

        tasks = [
            asyncio.ensure_future(self.schedule(semaphore, queue)) \
            for _ in range(len(self.metadata))
        ]

        loop.run_until_complete(asyncio.gather(*tasks))
        loop.run_until_complete(asyncio.sleep(0)) # Allow underline connection to close
        loop.close()
