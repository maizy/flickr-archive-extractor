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
import datetime
import pickle
import urllib.request
import urllib.error
import http
import time

__version__ = '0.0.3'

logger = logging.getLogger('flickr_archive_extractor')

IGNORED_JSONS_RE = re.compile(r'(account_profile|account_testimonials|apps_comments_part\d+|contacts_part\d+|'
                              r'faves_part\d+|followers_part\d+|galleries|galleries_comments_part\d+|'
                              r'group_discussions|groups|photos_comments_part\d+|received_flickrmail_part\d+|'
                              r'sent_flickrmail_part\d+|sets_comments_part\d+).json')


# args

def convert_archive_param(value):
    if value is not None and os.path.isdir(value):
        value = value.rstrip('/') + '/*.zip'
    return value


def check_path(path):
    if not os.path.exists(path):
        raise ValueError('{} not found'.format(path))
    return path


def parse_args():
    parser = argparse.ArgumentParser(description='flickr archive extractor v{}'.format(__version__))
    parser.add_argument('-v', '--verbose', action='store_true')

    subparsers = parser.add_subparsers(help='command --help', dest='command')

    check = subparsers.add_parser('check', help='check archives')
    check.add_argument('--archive', help='path to archives. globs may be used', action='append',
                       type=convert_archive_param, required=True)
    check.add_argument('--samples-size', default=10, type=int,
                       help='Size of displayed detailed samples for different kinds of data')

    upload = subparsers.add_parser('upload-to-google-photo', help='upload photos to google photos')
    upload.add_argument('--archive', help='path to archives. globs may be used', action='append',
                        type=convert_archive_param, required=True)
    upload.add_argument('--app-credentials', type=check_path, metavar='client_id.json',
                        help='path to app credentials in json format')
    upload.add_argument('--db', type=str, default=os.path.expanduser('~/.config/flickr_archive_extractor/db'),
                        help='path to file with database. will be created if missing.')

    args = parser.parse_args()
    if args.command is None:
        parser.error('command is required')
    return args


# parse archives

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


def sample(list_like, size):
    copy = list(list_like)
    random.shuffle(copy)
    return copy[0:size]


def map_sample(map_like, size):
    return dict(sample(map_like.items(), size))


def log_sample(sample, orig_size, format_func, what='items'):
    sample_size = len(sample)
    if sample_size > 0:
        logger.info(
            '⚠️ Found {len} {what} ({sample_size} random items):\n   * {list}{etc}'.format(
                len=orig_size,
                what=what,
                sample_size=sample_size,
                list='\n   * '.join(format_func(key) for key in sample),
                etc='\n   * ...' if orig_size > sample_size else ''
            )
        )
    else:
        logger.info("✅ There aren't %s", what)


class FlickrArchive:

    def __init__(self, zip_files, albums_file, items_metadata, items):
        self.zip_files = zip_files
        self.albums_file = albums_file
        self.items_metadata = items_metadata
        self.items = items
        self.without_metadata = None
        self.without_items = None
        self.unprocessed_videos_metadata = None
        self.matched = None
        self.albums = {}
        self.missed_items_in_albums = []
        self.wrong_items_in_albums = []
        self.item_to_albums_index = {}
        self.items_without_albums = []
        self._post_process()

    def __str__(self):
        return ('FlickrArchive<zip_files: {z}, items metadata: {pi}, items: {i}, albums: {al}>'
                .format(z=len(self.zip_files), pi=len(self.items_metadata), i=len(self.items),
                        al='found' if self.albums_file else 'not found'))

    def _post_process(self):
        items_keys = set(self.items.keys())
        metadata_keys = set(self.items_metadata.keys())

        matched_keys = items_keys.intersection(metadata_keys)

        matched_uid = {self.items[key].uid for key in matched_keys}

        self.unprocessed_videos_metadata = {key: meta for key, meta in self.items_metadata.items()
                                            if meta.is_unprocessed_video}
        unprocessed_videos_metadata_keys = set(self.unprocessed_videos_metadata.keys())

        without_metadata_unfiltered = items_keys - metadata_keys
        self.without_metadata = {key: self.items[key] for key in without_metadata_unfiltered
                                 if self.items[key].uid not in matched_uid}

        self.without_items = {key: self.items_metadata[key] for key in
                              (metadata_keys - items_keys - unprocessed_videos_metadata_keys)}

        self.matched = {key: ItemWithMetadata(self.items[key], self.items_metadata[key])
                        for key in matched_keys}

        if self.albums_file:
            albums_json = self.zip_files.parse_json(self.albums_file)
            for album_json in (albums_json.get('albums') or []):
                items = []
                album_id = album_json['id']
                for pid in (album_json.get('photos') or []):
                    if pid == '0':  # wrong photos ids
                        continue
                    if not re.match(r'^\d+$', pid):
                        self.wrong_items_in_albums.append((album_id, pid))
                    else:
                        pid_int = int(pid)
                        if pid_int in self.unprocessed_videos_metadata:
                            continue
                        elif pid_int not in self.matched:
                            self.missed_items_in_albums.append((album_id, pid_int))
                        else:
                            if pid_int not in self.item_to_albums_index:
                                self.item_to_albums_index[pid_int] = []
                            self.item_to_albums_index[pid_int].append(album_id)
                            items.append(pid_int)
                album = Album(
                    id=album_id,
                    title=album_json.get('title') or '',
                    description=albums_json.get('description') or '',
                    url=album_json['url'],
                    created=datetime.datetime.fromtimestamp(int(album_json['created'])),
                    updated=datetime.datetime.fromtimestamp(int(album_json['last_updated'])),
                    items_ids=items
                )
                if album_id in self.albums:
                    logger.warning('Duplicate album with id %s. %s, %s', album_id, self.albums[album_id], album)
                else:
                    self.albums[album_id] = album

        self.items_without_albums = [key for key in self.matched.keys() if key not in self.item_to_albums_index]

    @classmethod
    def build(cls, archives):
        zip_files = ZipFiles()
        albums_file = None
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
                    albums_file = file
                    continue

                item_match_1 = re.match(r'(?P<name>.+)_(?P<id>[0-9]+)_o\.(?P<ext>[a-z0-9]+)', file_path)
                item_match_2 = re.match(r'(?P<id>[0-9]+)_(?P<name>[0-9a-f]+)_o\.(?P<ext>[a-z0-9]+)', file_path)
                item_match_video = re.match(r'(?P<name>.+)_(?P<id>[0-9]+)\.(?P<ext>avi|mov|mp4|m4v)', file_path)

                item_match = item_match_1 or item_match_2 or item_match_video
                if item_match and item_match.group('ext') != 'json':
                    item_type, main_res, alt_res = cls._process_item_original_file(file, item_match, next(items_ids))
                    if main_res.id in items:
                        logger.warning('Duplicate item with id %s. %s, %s', main_res.id, items[main_res.id], main_res)
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
        return FlickrArchive(zip_files, albums_file, items_metadata, items)

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
            data=metadata,
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


Item = collections.namedtuple('Item', ['id', 'uid', 'file', 'name', 'type'])
ItemWithMetadata = collections.namedtuple('ItemWithMetadata', ['item', 'metadata'])
Album = collections.namedtuple('Album', ['id', 'title', 'description', 'url', 'created', 'updated', 'items_ids'])


class ItemMetadata(collections.namedtuple('ItemMetadata', ['id', 'data', 'metadata_file', 'original_name',
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

    def file_size(self, file: ArchiveFile):
        if file.archive_id not in self._zip_files:
            raise RuntimeError("archive with id '{}' not found".format(file.archive_id))
        zf = self._zip_files[file.archive_id]
        try:
            return zf.getinfo(file.path).file_size
        except KeyError as e:
            raise RuntimeError("path '{}' not found in archive".format(file.path)) from e

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


# db

def init_db(db_path):
    try:
        import sqlite3
    except ImportError:
        logger.critical("sqlite3 is required. it should be in the python stdlib")
        return None
    db = sqlite3.connect(db_path)
    tables = (db
              .execute(r"select name from sqlite_master where type in ('table','view') and name not like 'sqlite_%'")
              .fetchall())
    if not tables:
        init_tables(db)
    return db


def init_tables(db):
    db.execute("create table gphotos_token (token blob)")
    db.execute(
        "create table gphotos_albums ("
        "  seq_id integer primary key autoincrement,"
        "  album_id text not null,"
        "  status text not null default 'none',"
        "  google_id text"
        ")"
    )
    db.execute(
        "create table gphotos_items ("
        "  item_id integer,"
        "  album_id text,"
        "  status text not null default 'none',"
        "  google_id text,"
        "  primary key (item_id, album_id)"
        ")"
    )
    db.commit()
    return db


# google api

GOOGLE_PHOTOS_SCOPES = [
    'https://www.googleapis.com/auth/photoslibrary'
]


class GoogleAPILimitReached(Exception):
    pass


def init_google_photos_api(credentials_path, db):
    try:
        from googleapiclient import discovery
        from google_auth_oauthlib import flow
        from google.auth.transport import requests
    except ImportError:
        logger.critical('extra requirements needed for working with google photo.\n'
                        '  python3 -m pip install -r requirements-google-photo.txt')
        return None

    creds = None
    token_res = db.execute('select token from gphotos_token').fetchone()

    if token_res:
        creds = pickle.loads(token_res[0])

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(requests.Request())
        else:
            flow = flow.InstalledAppFlow.from_client_secrets_file(credentials_path, GOOGLE_PHOTOS_SCOPES)
            creds = flow.run_local_server()
        db.execute('delete from gphotos_token')
        db.execute('insert into gphotos_token (token) values(?)', (pickle.dumps(creds), ))

    return creds, discovery.build('photoslibrary', 'v1', credentials=creds)


def init_albums_to_upload_to_google_photos(albums_sorted, db):
    existed = 0
    created = 0
    albums_sorted = sorted(albums_sorted.items(), key=lambda x: x[1].created)
    for album_id, album in albums_sorted:
        seq_id_res = db.execute('select seq_id from gphotos_albums where album_id = ?', (album_id, )).fetchone()
        if not seq_id_res:
            db.execute('insert into gphotos_albums (album_id) values (?)', (album_id, ))
            created += 1
        else:
            existed += 1
    db.commit()
    return existed, created


def init_items_to_upload_to_google_photos(items_with_metadata, item_to_albums_index, db):
    existed = 0
    created = 0

    for index, i in enumerate(items_with_metadata.values()):
        albums_ids = item_to_albums_index.get(i.item.id, [None])
        if index != 0 and index % 1000 == 0:
            db.commit()
        for album_id in albums_ids:
            if album_id is not None:
                in_db = db.execute('select 1 from gphotos_items where item_id = ? and album_id = ?',
                                   (i.item.id, album_id)).fetchone()
            else:
                in_db = db.execute('select 1 from gphotos_items where item_id = ? and album_id is null',
                                   (i.item.id, )).fetchone()
            if not in_db:
                db.execute('insert into gphotos_items (item_id, album_id) values (?, ?)', (i.item.id, album_id))
                created += 1
            else:
                existed += 1
    db.commit()
    return existed, created


class RetryException(Exception):
    pass


def create_google_photos_album(album, album_status, album_google_id, gclient, db):
    import googleapiclient.errors
    if album_status == 'none':
        logging.info('Creating album "%s" (%s) (#%s)',
                     album.title, album.created.strftime('%Y-%m-%d'), album.id)
        retry = 0
        while retry < 5:
            if retry > 0:
                time.sleep(15.0 * retry)
            try:
                try:
                    resp = gclient.albums().create(body={'album': {'title': album.title}}).execute()
                    if 'id' not in resp:
                        raise RetryException('unable to get album id')
                    album_google_id = resp['id']
                except googleapiclient.errors.HttpError as e:
                    if e.resp.status == http.HTTPStatus.TOO_MANY_REQUESTS:
                        raise GoogleAPILimitReached()
                    else:
                        raise RetryException('unable to add item to album: {}'.format(e))
                db.execute("update gphotos_albums "
                           "set status = ?, google_id = ? "
                           "where album_id = ?", ('created', album_google_id, album.id))
                db.commit()
            except RetryException as e:
                retry += 1
                if retry < 5:
                    logging.warning('Retrying creating album "%s" (#%s). Error: %s', album.title, album.id, e)
                else:
                    logging.error('Unable to create album "%s" (#%s), skipping. Last error was: %s',
                                  album.title, album.id, e)

    return album_google_id


def http_request(req: urllib.request.Request):
    try:
        response = urllib.request.urlopen(req)
        return response.status, dict(response.getheaders()), response.read()
    except urllib.request.HTTPError as e:
        return e.code, dict(e.headers), e.read()
    except urllib.error.URLError:
        return 599, dict(), b''


def upload_item_to_google_photos(archive, album_id, album_google_id, item_with_meta, gclient, gcreds, db):
    import googleapiclient.errors
    item = item_with_meta.item
    meta = item_with_meta.metadata
    if album_id is not None:
        item_row = db.execute('select status, google_id from gphotos_items where item_id = ? and album_id = ?',
                              (item.id, album_id)).fetchone()
    else:
        item_row = db.execute('select status, google_id from gphotos_items where item_id = ? and album_id is null',
                              (item.id, )).fetchone()
    item_status = item_row[0]
    item_google_id = item_row[1]
    if item_status == 'none':
        retry = 0
        while retry < 5:
            try:
                meta_json = meta.data
                file_size = archive.zip_files.file_size(item.file)
                if file_size == 0:
                    fp = archive.zip_files.open_file(item.file)
                    file_size = len(fp.read())
                    fp.close()
                    if file_size == 0:
                        logger.warning('Unable to get photo size neither from zip metadata '
                                       'nor file content for item #%s',
                                       item.id)
                        raise RetryException('unable to get item size')
                file_name = '{i.name}.{i.type}'.format(i=item)
                logging.debug('Upload item #%s %s of %d bytes', item.id, file_name, file_size)
                req = urllib.request.Request(
                    method='POST',
                    url='https://photoslibrary.googleapis.com/v1/uploads',
                    headers={
                        'Authorization': 'Bearer {}'.format(gcreds.token),
                        'Content-Length': '0',
                        'X-Goog-Upload-Command': 'start',
                        'X-Goog-Upload-Content-Type': 'application/octet-stream',
                        'X-Goog-Upload-File-Name': file_name,
                        'X-Goog-Upload-Protocol': 'resumable',
                        'X-Goog-Upload-Raw-Size': str(file_size),
                    }
                )
                status, headers, _ = http_request(req)
                if status != http.HTTPStatus.OK:
                    raise RetryException('unable to start item upload')
                else:
                    fp = archive.zip_files.open_file(item.file)
                    upload_url = headers['X-Goog-Upload-URL']
                    chunk_size = int(headers['X-Goog-Upload-Chunk-Granularity'])
                    uploaded_bytes = 0
                    while True:
                        chunk_start = uploaded_bytes
                        uploaded_bytes += chunk_size
                        chunk_data = fp.read(chunk_size)
                        is_last_chunk = uploaded_bytes >= file_size
                        command = 'upload, finalize' if is_last_chunk else 'upload'
                        chunk_req = urllib.request.Request(
                            method='POST',
                            url=upload_url,
                            headers={
                                'Authorization': 'Bearer {}'.format(gcreds.token),
                                'Content-Length': str(len(chunk_data)),
                                'X-Goog-Upload-Command': command,
                                'X-Goog-Upload-Offset': str(chunk_start),
                            },
                            data=chunk_data
                        )
                        status, headers, body = http_request(chunk_req)
                        logger.debug('upload chunk of %d bytes => %d', len(chunk_data), status)
                        if status != http.HTTPStatus.OK:
                            raise RetryException('unable to upload chunk')
                        if is_last_chunk:
                            upload_token = body.decode('utf-8')
                            if len(upload_token) == 0:
                                raise RetryException('unable to get uploaded item token')
                            break
                body = {
                    'newMediaItems': [{
                        'description': meta_json.get('description') or file_name,
                        'simpleMediaItem': {
                            'uploadToken': upload_token
                        }
                    }]
                }
                if album_google_id is not None:
                    body['albumId'] = album_google_id
                try:
                    response = gclient.mediaItems().batchCreate(body=body).execute()
                    item_google_id = response['newMediaItemResults'][0]['mediaItem']['id']
                except googleapiclient.errors.HttpError as e:
                    if e.resp.status == http.HTTPStatus.TOO_MANY_REQUESTS:
                        raise GoogleAPILimitReached()
                    else:
                        raise RetryException('unable to add item to album: {}'.format(e))

                if album_id is not None:
                    db.execute("update gphotos_items "
                               "set status = 'uploaded', google_id = ? "
                               "where item_id = ? and album_id = ?", (item_google_id, item.id, album_id))
                else:
                    db.execute("update gphotos_items "
                               "set status = 'uploaded', google_id = ? "
                               "where item_id = ? and album_id is null", (item_google_id, item.id))
                db.commit()
            except RetryException as e:
                retry += 1
                if retry < 5:
                    logging.warning('Retrying uploading item %s (#%s). Error: %s', item.name, item.id, e)
                    time.sleep(15.0 * retry)
                else:
                    logging.error('Unable to upload item %s (#%s), skipping. Last error was: %s',
                                  item.name, item.id, e)

    return item_google_id


# actions


def load_archives_and_log_info(archive_globs):
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

    logger.info('Valid items found (items with matched metadata): {}'.format(len(archive.matched)))

    logger.info('Albums: {}'.format(len(archive.albums)))
    logger.info('Items with at least one album: {}'.format(len(archive.item_to_albums_index)))
    logger.info('Items without album: {}'.format(len(archive.items_without_albums)))

    return archive


def check(archive_globs, samples_size=30):
    archive = load_archives_and_log_info(archive_globs)

    logger.info('Items found: {}'.format(len(archive.items)))
    logger.info('Items metadata found: {}'.format(len(archive.items_metadata)))
    if archive.unprocessed_videos_metadata:
        logger.warning('⚠️  Unprocessed videos detected: {}'.format(len(archive.unprocessed_videos_metadata)))

    log_sample(
        map_sample(archive.without_metadata, samples_size).items(),
        len(archive.without_metadata),
        lambda pair: ('id={id}, archive={an}, path={f.path}'
                      .format(id=pair[1].id, f=pair[1].file, an=pair[1].file.archive_name(archive.zip_files))),
        'items without metadata'
    )

    if archive.without_items:
        logger.warning(
            ('⚠️  Found {len} items without an original file, check & download files by links bellow:\n   * {list}'
             .format(len=len(archive.without_items),
                     list='\n   * '.join('id={}: {}'.format(i.id, i.page_url)
                                         for i in archive.without_items.values())))
        )
    else:
        logger.info("✅ There aren't items without an original file")

    if not archive.albums:
        logger.error('⚠️  Albums not found')
    else:
        logger.info('Albums found: {}'.format(len(archive.albums)))
        log_sample(
            sample(archive.wrong_items_in_albums, samples_size),
            len(archive.wrong_items_in_albums),
            lambda pair: 'album_id={a}, item id={i}'.format(a=pair[0], i=pair[1]),
            'wrong items in albums'
        )
        log_sample(
            sample(archive.missed_items_in_albums, samples_size),
            len(archive.missed_items_in_albums),
            lambda pair: ('album_id={a}, item id={i}, album url={url}'
                          .format(a=pair[0], i=pair[1], url=archive.albums[pair[0]].url)),
            'missed items in albums'
        )


def upload_to_google_photos(archive_globs, db_path):
    archive = load_archives_and_log_info(archive_globs)

    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir, mode=0o755, exist_ok=True)
    db = init_db(db_path)
    if db is None:
        return 1

    gcreds, gclient = init_google_photos_api(args.app_credentials, db)
    if gclient is None:
        return 1

    logger.info('Preparing to upload albums ...')
    albums_existed, albums_created = init_albums_to_upload_to_google_photos(archive.albums, db)

    if albums_existed > 0:
        logger.info('Albums to upload found in DB: %d', albums_existed)
    if albums_created > 0:
        logger.info('Albums to upload added: %d', albums_created)

    logger.info('Preparing to upload items ...')
    items_existed, items_created = init_items_to_upload_to_google_photos(
        archive.matched, archive.item_to_albums_index, db
    )

    if items_existed > 0:
        logger.info('Items to upload found in DB: %d', items_existed)
    if items_created > 0:
        logger.info('Items to upload added: %d', items_created)

    albums = db.execute('select album_id, status, google_id from gphotos_albums order by seq_id').fetchall()
    skipped_items = 0
    skipped_albums = 0
    for album_row in albums:
        album_id = album_row[0]
        album = archive.albums[album_id]
        total_items = len(album.items_ids)

        album_google_id = create_google_photos_album(album, album_status=album_row[1], album_google_id=album_row[2],
                                                     gclient=gclient, db=db)

        if album_google_id is None:
            skipped_albums += 1
            skipped_items += total_items
            continue

        logging.info('Uploading %d items for album "%s" (%s)',
                     total_items, album.title, album.created.strftime('%Y-%m-%d'))

        for index, item_id in enumerate(album.items_ids):
            item = archive.matched[item_id]
            if upload_item_to_google_photos(archive, album_id, album_google_id, item, gclient, gcreds, db) is None:
                skipped_items += 1
            if index != 0 and index % 10 == 0:
                logging.info('.. %d / %d', index, total_items)
        logging.info('.. %d / %d - Done', total_items, total_items)

    total_items = len(archive.items_without_albums)
    logging.info('Uploading %d items without albums', total_items)

    for index, item_id in enumerate(archive.items_without_albums):
        item = archive.matched[item_id]
        if upload_item_to_google_photos(archive, None, None, item, gclient, gcreds, db) is None:
            skipped_items += 1
        if index != 0 and index % 10 == 0:
            logging.info('.. %d / %d', index, total_items)

    if skipped_albums > 0:
        logging.error('⚠️ Unable to upload %d albums, try running script again')
    if skipped_items > 0:
        logging.error('⚠️ Unable to upload %d items, try running script again')
    logging.info('.. %d / %d - Done', total_items, total_items)
    db.commit()

    logging.info('🎉 Job is done')
    db.close()
    return 0


if __name__ == '__main__':
    args = parse_args()
    if args.verbose:
        logging_format = '%(asctime)s [%(name)s] %(levelname).1s %(message)s'
    else:
        logging_format = '%(asctime)s %(levelname).1s %(message)s'
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        stream=sys.stderr,
        format=logging_format,
        datefmt='%H:%M:%S'
    )
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

    if not args.verbose:
        logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)

    if args.command == 'check':
        check(args.archive, args.samples_size)
    elif args.command == 'upload-to-google-photo':
        try:
            upload_to_google_photos(args.archive, args.db)
        except GoogleAPILimitReached as e:
            logger.error("😞 Looks like you've reached Google API limits. Try to continue after 24h.")

    else:
        print('Unknown command {}'.format(args.command))
        sys.exit(2)
