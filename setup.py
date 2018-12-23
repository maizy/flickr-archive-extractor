from setuptools import setup

setup(
    name='flickr-archive-extractor',
    version='0.0.1',
    install_requires=[],
    tests_require=['nose>=1.3', 'pycodestyle'],
    test_suite='nose.collector',
    scripts=['flickr_archive_extractor.py'],
    author='Nikita Kovaliov',
    author_email='nikita@maizy.ru',
    description='flickr archive extractor',
    license='Apache License 2.0',
    keywords='flickr',
    url='https://github.com/maizy/flickr-archive-extractor',
)
