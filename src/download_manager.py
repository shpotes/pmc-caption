import io
import ftplib
import os
import pathlib
import re
import tarfile
from typing import Optional
import uuid
import pandas as pd
from PIL import Image
import pubmed_parser as pp
from tqdm import tqdm

class Downloader:
    def __init__(
            self,
            data_dir,
            metadata_path: str,
            num_samples: Optional[int] = None
    ):
        self.data_dir = pathlib.Path(data_dir)
        self.metadata = self._prepare_metadata(metadata_path, num_samples)
        self.client = ftplib.FTP('ftp.ncbi.nlm.nih.gov')
        self.prepare_download()

    def _filter_downloaded_files(self, metadata: pd.DataFrame) -> pd.DataFrame:
        return metadata

    def _prepare_metadata(
            self,
            metadata_path: str,
            num_samples: Optional[int] = None
    ) -> pd.DataFrame:
        """
        preprocess pmc metadata file
        """
        meta = pd.read_csv(metadata_path)
        meta.columns = ['fname', 'citation', 'accession_id', 'last_update', 'pmid', 'license']
        meta = meta[meta['license'] != 'NO-CC CODE'].reset_index(drop=True)
        meta.fname = meta.fname.apply(lambda x: '/'.join(x.split('/')[1:]))
        meta.last_update = pd.to_datetime(meta.last_update)

        if num_samples:
            meta = meta.sample(num_samples)
        else:
            meta = meta.sample(frac=1)

        meta = self._filter_downloaded_files(meta)

        if meta.empty:
            raise NotImplementedError

        return meta


    def prepare_download(self):
        self.client.login()
        self.client.cwd('pub/pmc/oa_package/')

    def download(self):
        for fname in self.metadata.fname:
            callback = io.BytesIO()
            self.client.retrbinary(f'RETR {fname}', callback.write, blocksize=33554432)
            yield fname, callback

    def extract(self, tar_buffer):
        tar_buffer.seek(0)

        tar = tarfile.open(fileobj=tar_buffer)
        members = tar.getmembers()

        imgs_files = re.compile(r'.*(\.gif|jpe?g|tiff?|png|webp|bmp)$')
        text_file = re.compile(r'.*(\.nxml)$')

        imgs = {}

        for mem in members:
            if imgs_files.match(mem.name):
                img_ref = os.path.basename(mem.name)
                img_ref = os.path.splitext(img_ref)[0]
                imbuffer = tar.extractfile(mem).read()
                imgs[img_ref] = imbuffer

            if text_file.match(mem.name):
                text = tar.extractfile(mem.name).read().decode('utf-8')
                caption = pp.parse_pubmed_caption(text)

        return imgs, caption

    def save_data(self, imgs, caption):
        for capt in caption:
            imbuffer = imgs[capt['graphi_ref']]
            image = Image.open(io.BytesIO(imbuffer))
            image.save(self.data_dir / f'{capt["pmc"]}_{uuid.uuid4()}.png')

            # TODO: query db

    def download_and_extract(self):
        for _, fbuffer in tqdm(self.download()):
            imgs, caption = self.extract(fbuffer)
            self.save_data(imgs, caption)
