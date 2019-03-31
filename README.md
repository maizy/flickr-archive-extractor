# flickr archive extractor

Tool for checking & extracting flickr archives.


## Requirements

* python 3.5+

## Usage

```
python3 flickr_archive_extractor.py --help
python3 flickr_archive_extractor.py action --help
```

Available actions:

* `check` - check archive integrity, find missing items
* `upload-to-google-photo` - upload library to google photo. 
   Because of API limits may require several days to complete. Upload progress will be stored on disk.

## How to upload archive to Google Photos

* install additional python requirements: `python3 -m pip install -r requirements-google-photo.txt`
* [create project and enable google photo API](https://console.developers.google.com/projectselector2/apis/api/photoslibrary.googleapis.com/overview)
  for user whom will be used to upload photos
* [create consent screen](https://console.developers.google.com/apis/credentials/consent)
  * fill _Application name_
  * ignore other fields
* [create credentials](https://console.developers.google.com/apis/credentials/wizard)
  * _Which API are you using?_: Photo Library API
  * _Where will you be calling the API from?_: Other UI
  * _What data will you be accessing?_: User data
  * _Name_: any value, for ex. `flick-to-google-photo`
* download credentials in json format
* see `python3 flickr_archive_extractor.py upload-to-google-photo --help` for more

## License

Apache License 2.0
