
import logging
import zipfile
from io import BytesIO, StringIO

import csv
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def unzip_csv_files_in_gcs(source_bucket, obj_prefix, target_bucket=None, remove_original=False):
    """
    Download any zip files contained in the gs://{source_bucket/obj_prefix location
    Unzip them
    If there's only 1 row (the header) in the file, skip it
    Optionally remove the original zip file after upload
    """
    gcs_hook = GCSHook()
    file_list = gcs_hook.list(source_bucket,prefix=obj_prefix)
    for file_obj in file_list:
        print(file_obj)
        print(file_obj[-4:])
        if file_obj[-4:] == ".zip":
            file_contents = gcs_hook.download(file_obj,source_bucket)
            file_bytes = BytesIO(file_contents)
            zip_file = zipfile.ZipFile(file_bytes)
            for filename in zip_file.namelist():
                csv_file = StringIO(zip_file.read(filename).decode("utf-8"))
                reader = csv.reader(csv_file)
                empty = True
                for i, _ in enumerate(reader):
                    if i:  # found the second row
                        empty = False
                if not empty:
                    logging.info("%s has data", filename)
                    if target_bucket is None:
                        gcs_hook.upload(source_bucket,f"{obj_prefix}/{filename}",data=zip_file.read(filename))
                    else:
                        gcs_hook.upload(target_bucket,f"{obj_prefix}/{filename}",data=zip_file.read(filename))
                    logging.info("Unzipped %s", file_obj)
                    if remove_original:
                        gcs_hook.delete(source_bucket,file_obj)
                else:
                    logging.info("%s is empty", filename)
