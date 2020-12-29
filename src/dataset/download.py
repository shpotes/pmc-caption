import io
import pathlib
import ftplib
from tqdm import tqdm

_BASE_URL = 'ftp.ncbi.nlm.nih.gov'

class Downloader:
    def __init__(self, data_dir):
        self.data_dir = pathlib.Path(data_dir)
        self._pmc = ftplib.FTP(_BASE_URL)
        self._pmc.login('anonymous', '')
        self._pmc.cwd('pub/pmc/oa_package/')

    def download_single_file(self, fname, save=True):
        if save:
            callback = open(self.data_dir / fname, 'wb').write
        else:
            callback = io.BytesIO()

        if 1:
        #try:
            self._pmc.retrbinary(f'RETR {fname}', callback, blocksize=33554432)
            if not save:
                return callback
            else:
                return 0

    def download_folder(self, fold_path, save=True):
        local_dir = self.data_dir / fold_path
        local_dir.mkdir(parents=True, exist_ok=True)

        iter_ftp = self._pmc.nlst(fold_path)
        for fname in tqdm(iter_ftp):
            self.download_single_file(fname, save=save)

    def list_folder(self, fold_path):
        return self._pmc.nlst(fold_path)

if __name__ == '__main__':
    root_dir = pathlib.Path(__file__).absolute()
    data_dir = root_dir.parents[2] / 'raw'

    down = Downloader(data_dir)
    down.download_folder('00/00')
