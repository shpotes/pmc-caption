import sys
import pathlib

ROOT_DIR = pathlib.Path(__file__).absolute().parents[1]
sys.path.append(str(ROOT_DIR))

from src.dataset.download import Downloader

if __name__ == '__main__':
    data_dir = ROOT_DIR / 'data' / 'raw'
    pmc = Downloader(data_dir)
    for fold in pmc.list_folder('00/'):
        pmc.download_folder(fold)
