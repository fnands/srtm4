#!/usr/bin/env python
# vim: set fileencoding=utf-8
# pylint: disable=C0103

"""
Module to download terrain digital elevation models from the SRTM 90m DEM.

Copyright (C) 2016, Carlo de Franchis <carlo.de-franchis@ens-cachan.fr>
"""

from __future__ import print_function
import zipfile
import sys
import os

import requests
from requests.adapters import HTTPAdapter, Retry, RetryError
import filelock
from cloudpathlib import CloudPath

SRTM_URL = 's3://srtm-dems/srtm_5x5/TIFF'


def _requests_retry_session(
        retries=5,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 503, 504),
):
    """
    Makes a requests object with built-in retry handling with
    exponential back-off on 5xx error codes.
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def download(to_file, from_url):
    """
    Download a file from the internet.

    Args:
        to_file: path where to store the downloaded file
        from_url: url of the file to download

    Raises:
        RetryError: if the `get` call exceeds the number of retries
            on 5xx codes
        ConnectionError: if the `get` call does not return a 200 code
    """
    remote_file = CloudPath(from_url)
    remote_file.download_to(to_file)


def get_srtm_tile(srtm_tile, out_dir):
    """
    Download and unzip an srtm tile from the s3.

    Args:
        srtm_tile: string following the pattern 'srtm_%02d_%02d', identifying
            the desired strm tile
        out_dir: directory where to store and extract the srtm tiles
    """

    output_dir = os.path.abspath(os.path.expanduser(out_dir))
    try:
        os.makedirs(output_dir)
    except OSError:
        pass

    print("name output")

    # download the zip file
    srtm_tile_url = '{}/{}.zip'.format(SRTM_URL, srtm_tile)
    zip_path = os.path.join(out_dir, '{}.zip'.format(srtm_tile))
    #lock_zip = filelock.FileLock(srtm_zip_download_lock)
    #lock_zip.acquire()

    print("name output")

    if os.path.exists(zip_path):
        print('zip already exists')
        # Only possibility here is that the previous process was cut short

    
    try:
        print(f"Downloading {srtm_tile_url} to {zip_path}")
        download(zip_path, srtm_tile_url)
    except (ConnectionError, RetryError) as e:
        #lock_zip.release()
        raise e

    #lock_tif = filelock.FileLock(srtm_tif_write_lock)
    #lock_tif.acquire()
    print(os.listdir(out_dir))
    print(zipfile.is_zipfile(zip_path))
    # extract the tif file
    if zipfile.is_zipfile(zip_path):
        z = zipfile.ZipFile(zip_path, 'r')
        z.extract('{}.tif'.format(srtm_tile), out_dir)
        # remove the zip file
        os.remove(zip_path)
    else:
        print('{} not available'.format(srtm_tile))

    print(os.listdir(out_dir))

