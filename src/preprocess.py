import io
import pathlib
import re
import tarfile
import uuid
from typing import Tuple
from tqdm import tqdm
from PIL import Image
import pubmed_parser as pp
import yaml

def _extract_img(
    tar_file: tarfile.TarFile, 
    member: tarfile.TarInfo
):
    """
    Extract image buffer
    """
    img_buf = tar_file.extractfile(member).read()
    return Image.open(io.BytesIO(img_buf))

def _extract_text(tar_file, member):
    """
    Extract nxml file and return image captions
    """
    text = tar_file.extractfile(member).read().decode('utf-8')
    return pp.parse_pubmed_caption(text)

def extract(tar_path):
    _regex = {
        'img': re.compile(r'.*(\.gif|jpe?g|tiff?|png|webp|bmp)$'),
        'text': re.compile(r'.*(\.nxml)$')
    }
    
    tar_file = tarfile.open(tar_path, mode='r:*')
    images = {}

    for member in tar_file.getmembers():
        if _regex['img'].match(member.name):
            img_ref = pathlib.Path(member.name).stem
            img = _extract_img(tar_file, member)
            
            if img_ref in images and images[img_ref].size > img.size:
                continue

            images[img_ref] = img
            
        if _regex['text'].match(member.name):
            captions = _extract_text(tar_file, member)

    return images, captions

def run_extracter(data_dir, limit_size=1e5):
    db = []

    target_dir = data_dir / 'raw'
    image_dir = data_dir / 'images'

    for sample in tqdm(target_dir.glob('*/*/*/*.tar.gz'), total=10312):
        try:
            images, captions = extract(sample)
        except:
            pass
        if captions:
            for capt in captions:
                filestem = str(uuid.uuid4())
                capt['filestem'] = filestem
                images[capt['graphic_ref']].save(image_dir / f'{filestem}.png')
            
            db.extend(captions)
        
        if len(db) > limit_size:
            with open(data_dir / 'metadata.yaml', 'a') as buffer:
                buffer.write(yaml.dump(db))
            db = []

