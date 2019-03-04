#!/usr/bin/env python3
# encoding: utf-8
import sys
import os.path
import glob
import json
import argparse
import zipfile
import collections
import re
import logging
import random

__version__ = '0.0.2'

logger = logging.getLogger()

IGNORED_JSONS_RE = re.compile(r'(account_profile|account_testimonials|apps_comments_part\d+|contacts_part\d+|'
                              r'faves_part\d+|followers_part\d+|galleries|galleries_comments_part\d+|'
                              r'group_discussions|groups|photos_comments_part\d+|received_flickrmail_part\d+|'
                              r'sent_flickrmail_part\d+|sets_comments_part\d+).json')


def convert_archive_param(value):
    if value is not None and os.path.isdir(value):
        value = value.rstrip('/') + '/*.zip'
    return value


def parse_args():
    parser = argparse.ArgumentParser(description='flickr archive extractor v{}'.format(__version__))
    parser.add_argument('-v', '--verbose', action='store_true')

    subparsers = parser.add_subparsers(help='command --help', dest='command')
    check = subparsers.add_parser('check', help='check archives')
    check.add_argument('--archive', help='path to archives. globs may be used', action='append',
                       type=convert_archive_param, required=True)
    check.add_argument('--samples-size', default=10, type=int,
                       help='Size of displayed detailed samples for different kinds of data')

    args = parser.parse_args()
    if args.command is None:
        parser.error('command is required')
    return args


def list_archives(archive_globs):
    archives_paths = []
    wrong_paths = []
    for pattern in archive_globs:
        for path in glob.iglob(pattern):
            if os.path.exists(path) and zipfile.is_zipfile(path):
                archives_paths.append(path)
            else:
                wrong_paths.append(path)
    return archives_paths, wrong_paths


class FlickrArchive(collections.namedtuple('Archives', ['zip_files', 'albums', 'items_metadata', 'items'])):
    def __str__(self):
        return ('FlickrArchive<zip_files: {z}, items metadata: {pi}, items: {i}, albums: {al}>'
                .format(z=len(self.zip_files), pi=len(self.items_metadata), i=len(self.items),
                        al='found' if self.albums else 'not found'))

    @classmethod
    def build(cls, archives):
        zip_files = ZipFiles()
        albums = None
        items_metadata = {}
        items = {}
        types = set()
        items_ids = iter(range(0, 10**7))
        for archive_id, archive in enumerate(archives):
            zf = zipfile.ZipFile(archive)
            zip_files.add_archive(archive_id, zf)
            for file_path in zf.namelist():
                file = ArchiveFile(archive_id=archive_id, path=file_path)
                if file_path == 'albums.json':
                    albums = file
                    continue

                item_match_1 = re.match(r'(?P<name>.+)_(?P<id>[0-9]+)_o\.(?P<ext>[a-z0-9]+)', file_path)
                item_match_2 = re.match(r'(?P<id>[0-9]+)_(?P<name>[0-9a-f]+)_o\.(?P<ext>[a-z0-9]+)', file_path)
                item_match_video = re.match(r'(?P<name>.+)_(?P<id>[0-9]+)\.(?P<ext>avi|mov|mp4|m4v)', file_path)

                item_match = item_match_1 or item_match_2 or item_match_video
                if item_match and item_match.group('ext') != 'json':
                    item_type, main_res, alt_res = cls._process_item_original_file(file, item_match, next(items_ids))
                    if main_res.id in items:
                        logging.warning('Duplicate item with id %s. %s, %s', main_res.id, items[main_res.id], main_res)
                    else:
                        items[main_res.id] = main_res
                    if alt_res is not None and alt_res.id not in items:
                        items[alt_res.id] = alt_res
                    continue

                item_metadata_match = re.match(r'photo_(?P<id>[0-9]+).json', file_path)
                if item_metadata_match:
                    item_metadata = cls._process_item_metadata(file, zip_files, item_metadata_match)
                    if item_metadata.id in items_metadata:
                        logging.warning('Duplicate item info with id %s. %s, %s',
                                        item_metadata.id, items_metadata[item_metadata.id], item_metadata)
                    else:
                        items_metadata[item_metadata.id] = item_metadata
                    continue

                if not IGNORED_JSONS_RE.match(file_path):
                    logging.warning('Unknown file in archive: %s', file)

        logging.debug('Item types in archive: {}'.format(', '.join(types)))
        return FlickrArchive(zip_files, albums, items_metadata, items)

    @classmethod
    def _process_item_original_file(cls, file, item_match, uid):
        item_type = item_match.group('ext')
        item_id = int(item_match.group('id'))
        name = item_match.group('name')
        main_item = Item(item_id, uid, file=file, name=name, type=item_type)
        alt_item = None
        if re.match(r'^\d+$', name) is not None:
            # swap name & item id
            alt_item = Item(id=int(name), uid=uid, file=file, name=str(item_id), type=item_type)
        return item_type, main_item, alt_item

    @classmethod
    def _process_item_metadata(cls, file, zip_files, item_match):
        photo_id = int(item_match.group('id'))
        metadata = zip_files.parse_json(file)
        return ItemMetadata(
            photo_id,
            metadata_file=file,
            original_name=metadata['original'],
            albums=metadata['albums'],
            page_url=metadata['photopage']
        )


class ArchiveFile(collections.namedtuple('ArchiveFile', ['archive_id', 'path'])):

    @property
    def base_name(self):
        return self.path.split('/')[-1]

    def archive_name(self, zip_files):
        return zip_files.archive_by_id(self.archive_id).filename.split('/')[-1]


class Item(collections.namedtuple('Item', ['id', 'uid', 'file', 'name', 'type'])):
    pass


class ItemMetadata(collections.namedtuple('ItemMetadata', ['id', 'metadata_file', 'original_name',
                                                           'albums', 'page_url'])):
    @property
    def is_unprocessed_video(self):
        return self.original_name.split('/')[-1] == 'video_encoding.jpg'


class ZipFiles:
    def __init__(self):
        self._zip_files = {}

    def add_archive(self, archive_id, zip_file):
        self._zip_files[archive_id] = zip_file

    def archive_by_id(self, archive_id):
        return self._zip_files.get(archive_id)

    def open_file(self, file: ArchiveFile, mode='r'):
        if file.archive_id not in self._zip_files:
            raise RuntimeError("archive with id '{}' not found".format(file.archive_id))

        zf = self._zip_files[file.archive_id]
        try:
            zf.getinfo(file.path)
        except KeyError as e:
            raise RuntimeError("path '{}' not found in archive".format(file.path)) from e
        return zf.open(file.path, mode)

    def get_file_content(self, file):
        return self.open_file(file).read()

    def parse_json(self, file):
        return json.loads(self.get_file_content(file).decode('utf-8'))

    def __len__(self) -> int:
        return len(self._zip_files)


def check(archive_globs, samples_size=30):
    archives_paths, wrong_paths = list_archives(archive_globs)

    logger.info('Archives globs:\n * {}'.format('\n * '.join(archive_globs)))
    logger.info('Archives paths found: {}'.format(len(archives_paths)))
    if archives_paths and logger.isEnabledFor(logging.DEBUG):
        logger.debug('Archives paths:\n * {}'.format('\n * '.join(archives_paths)))
    if wrong_paths:
        logger.warning('Wrong paths:\n * {}'.format('\n * '.join(wrong_paths)))

    logger.info('Indexing archives ...')
    archive = FlickrArchive.build(archives_paths)
    logger.info('Index has been built')

    logger.info('Items found: {}'.format(len(archive.items)))
    logger.info('Items metadata found: {}'.format(len(archive.items_metadata)))

    items_keys = set(archive.items.keys())
    metadata_keys = set(archive.items_metadata.keys())

    matched_keys = items_keys.intersection(metadata_keys)
    logger.info('Items with matched metadata: {}'.format(len(matched_keys)))

    matched_uid = {archive.items[key].uid for key in matched_keys}

    unprocessed_videos_metadata = {pid: meta for pid, meta in archive.items_metadata.items()
                                   if meta.is_unprocessed_video}

    unprocessed_videos_metadata_keys = set(unprocessed_videos_metadata.keys())
    logger.info('Unprocessed videos detected: {}'.format(len(unprocessed_videos_metadata_keys)))

    without_metadata_unfiltered = items_keys - metadata_keys
    without_metadata = {key for key in without_metadata_unfiltered
                        if archive.items[key].uid not in matched_uid}

    without_metadata_sample_pids = list(without_metadata)
    random.shuffle(without_metadata_sample_pids)
    without_metadata_sample_pids = without_metadata_sample_pids[0:samples_size]
    without_metadata_sample = [archive.items[pid] for pid in without_metadata_sample_pids]

    without_items = [archive.items_metadata[pid] for pid in
                     (metadata_keys - items_keys - unprocessed_videos_metadata_keys)]

    if without_metadata:
        logger.info(
            'Found {len} items without metadata (up to {sample} random items):\n   * {list}{etc}'.format(
                len=len(without_metadata),
                sample=samples_size,
                list='\n   * '.join(
                    'id={id}, archive={an}, path={f.path}'.format(
                        id=item.id,
                        f=item.file,
                        an=item.file.archive_name(archive.zip_files)
                    )
                    for item in without_metadata_sample
                ),
                etc='\n...' if len(without_metadata) > samples_size else ''
            )
        )

    if without_items:
        logger.info(
            'Found {len} items without an original file, check & download files by links bellow:\n   * {list}'.format(
                len=len(without_items),
                list='\n   * '.join('id={}: {}'.format(i.id, i.page_url) for i in without_items)
            )
        )


if __name__ == '__main__':
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        stream=sys.stderr,
        format='%(asctime)s %(levelname).1s %(message)s',
        datefmt='%H:%M:%S'
    )

    if args.command == 'check':
        if not check(args.archive, args.samples_size):
            sys.exit(1)
    else:
        print('Unknown command {}'.format(args.command))
        sys.exit(2)
