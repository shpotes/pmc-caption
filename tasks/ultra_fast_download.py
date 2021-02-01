import argparse
import pathlib
import sys
import logging
import time
import pandas as pd

ROOT_DIR = pathlib.Path(__file__).absolute().parents[1]
sys.path.append(str(ROOT_DIR))

from src import Downloader


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='PMCaption downloader')
    parser.add_argument('-d','--data_dir', type=str, help='root data dir')
    parser.add_argument('--log', action='store_true', help='activate logging')
    parser.add_argument(
        '-n', '--num_samples',
        type=int,
        help='number of samples to download',
        default=None
    )

    args = parser.parse_args()

    if args.log:
        logging.basicConfig(level=logging.DEBUG)

    metadata = pd.read_csv(f'{args.data_dir}oa_file_list.csv')

    dl_manager = Downloader(
        args.data_dir,
        metadata,
        args.num_samples
    )

    num_examples = 0

    while num_examples < args.num_samples:
        try:
            dl_manager.download()
            break
        except Exception as e:
            meta, num_examples = dl_manager.update_metadata(
                metadata,
                args.num_samples
            )

            dl_manager.metadata = meta

            print('\n' * 5, num_examples, '\n' * 5)
            print(e)

            time.sleep(30)
