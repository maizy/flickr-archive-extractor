#!/usr/bin/env python3
# encoding: utf-8
import sys
import os.path
import glob
import argparse
import zipfile
import collections
import re
import logging

__version__ = '0.0.1'

logger = logging.getLogger()

IGNORED_JSONS_RE = re.compile(r'(account_profile|account_testimonials|apps_comments_part\d+|contacts_part\d+|'
                              r'faves_part\d+|followers_part\d+|galleries|galleries_comments_part\d+|'
                              r'group_discussions|groups|photos_comments_part\d+|received_flickrmail_part\d+|'
                              r'sent_flickrmail_part\d+|sets_comments_part\d+).json')


def parse_args():
    parser = argparse.ArgumentParser(description='flickr archive extractor v{}'.format(__version__))
    parser.add_argument('-v', '--verbose', action='store_true')

    subparsers = parser.add_subparsers(help='command --help', dest='command')
    check = subparsers.add_parser('check', help='check archives')
    check.add_argument('--archive', help='path to archives. globs may be used', action='append', required=True)

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


class FlickrArchive(collections.namedtuple('Archives', ['zip_files', 'albums', 'items_infos_index', 'items'])):
    def __str__(self):
        return ('FlickrArchive<zip_files: {z}, items infos: {pi}, items: {i}, albums: {al}>'
                .format(z=len(self.zip_files), pi=len(self.items_infos_index), i=len(self.items),
                        al='found' if self.albums else 'not found', ))


ArchiveFile = collections.namedtuple('ArchiveFile', ['archive_id', 'path'])
ArchiveItem = collections.namedtuple('ArchiveItem', ['id', 'file', 'type'])


def build_archives_index(archives):
    zip_files = {}
    albums = None
    items_infos = {}
    items = {}
    types = set()
    for archive_id, archive in enumerate(archives):
        zf = zipfile.ZipFile(archive)
        zip_files[archive_id] = zf
        for file_path in zf.namelist():
            file = ArchiveFile(archive_id=archive_id, path=file_path)
            if file_path == 'albums.json':
                albums = file
                continue
            item_match = re.match(r'(.+)\.([a-z0-9]+)', file_path)
            if item_match and item_match.group(2) != 'json':
                types.add(item_match.group(2))
                item_name = item_match.group(1)
                item_id = item_name  # TODO: parse id
                item = ArchiveItem(item_id, file, item_match.group(2))
                if item.id in items:
                    logging.warning('Duplicate item with id %s. %s, %s', item_id, items[item.id], item)
                else:
                    items[item.id] = item
                continue
            photo_info_match = re.match(r'photo_([0-9]+).json', file_path)
            if photo_info_match:
                photo_id = int(photo_info_match.group(1))
                if photo_id in items_infos:
                    logging.warning('Duplicate item info with id %s. %s, %s', photo_id, items_infos[photo_id], file)
                else:
                    items_infos[photo_id] = file
                continue
            if not IGNORED_JSONS_RE.match(file_path):
                logging.debug('Unknown file in archive: %s', file)
    logging.debug('Item types in archive: {}'.format(', '.join(types)))
    return FlickrArchive(zip_files, albums, items_infos, items)


def check(archive_globs):
    archives_paths, wrong_paths = list_archives(archive_globs)

    print('Archives globs:\n * {}'.format('\n * '.join(archive_globs)))
    if archives_paths:
        print('Archives paths:\n * {}'.format('\n * '.join(archives_paths)))
    if wrong_paths:
        print('Wrong paths:\n * {}'.format('\n * '.join(wrong_paths)))

    archive = build_archives_index(archives_paths)
    print(archive)


if __name__ == '__main__':
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, stream=sys.stderr)

    if args.command == 'check':
        if not check(args.archive):
            sys.exit(1)
    else:
        print('Unknown command {}'.format(args.command))
        sys.exit(2)
