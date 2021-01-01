import argparse
import sys

ROOT_DIR = pathlib.Path(__file__).absolute().parents[1]
sys.path.append(str(ROOT_DIR))

from src import Downloader

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='PMCaption downloader')
    parser.add_argument('data_dir', type=str, help='image target directory')
    parser.add_argument('metadata', type=str, help='metadata file path')
    parser.add_argument(
        'num_samples',
        type=int,
        help='number of samples to download',
        default=None
    )

    args = parser.parse_args()

    dl_manager = Downloader(
        args.data_dir,
        args.metadata,
        args.num_samples
    )

    dl_manager.download_and_extract()
