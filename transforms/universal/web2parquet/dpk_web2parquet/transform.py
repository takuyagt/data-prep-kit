# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import time
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.transform import AbstractTableTransform
from dpk_web2parquet.utils import get_file_info

from dpk_connector import crawl, shutdown

user_agent = "Mozilla/5.0 (X11; Linux i686; rv:125.0) Gecko/20100101 Firefox/125.0"

logger = get_logger(__name__)

class Web2ParquetTransform(AbstractTableTransform):
    """
    Implements a simple copy of a pyarrow Table.
    """

    def __init__(self, **kwargs):
        """
        Initialize based on the dictionary of configuration information.
        This is generally called with configuration parsed from the CLI arguments defined
        by the companion runtime, NOOPTransformRuntime.  If running inside the RayMutatingDriver,
        these will be provided by that class with help from the RayMutatingDriver.
        """
        # Make sure that the param name corresponds to the name used in apply_input_params method
        # of NOOPTransformConfiguration class
        super().__init__(kwargs)
        self.seed_urls = kwargs.get("urls", [])
        self.depth = kwargs.get("depth", 1)
        self.downloads = kwargs.get("downloads", 10)
        self.allow_mime_types = kwargs.get("mime_types", ["application/pdf","text/html","text/markdown","text/plain"])

        assert self.seed_urls.length, "Must specify a URL to crawl. Url cannot be None"
        
        self.count = 0
        self.docs = []
        # create a data access object for storing files locally
        self.dao = None 

     def on_download(self, url: str, body: bytes, headers: dict) -> None:
        """
        Callback function called when a page has been downloaded.
        You have access to the request URL, response body and headers.
        """
        logger.debug(f"url: {url}, headers: {headers}, body: {body[:64]}")
        self.count += 1
        file_info = parse_headers(headers=headers, url=url)
        doc = headers
        doc['url'] = url
        doc['filename'], doc['content_type'] = get_file_info(headers)
        doc['content'] = body
        self.docs.append(doc)
        try:
            if self.dao:
                self.dao.sav()
        except:
            pass

    def transform(self, table: pa.Table=None, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        """
        Put Transform-specific to convert one Table to 0 or more tables. It also returns
        a dictionary of execution statistics - arbitrary dictionary
        This implementation makes no modifications so effectively implements a copy of the
        input parquet to the output folder, without modification.
        """
        start_time = time.time()
        crawl(
            self.seed_urls,
            self.on_download,
            user_agent=user_agent,
            depth_limit=self.depth,
            allow_mime_types=self.allow_mime_types
        )  # blocking call
        # Shutdown all crawls
        shutdown()

        end_time = time.time()      
        table = pa.Table.from_pylist(self.docs)
        metadata = {
            "count": self.count,
            "start time": start_time,
            "end time": end_time
            }
        logger.info(f"Crawling is completed in {end_time - start_time:.2f} seconds") 
        logger.info(f"{metadata = }")
        return [table], metadata



