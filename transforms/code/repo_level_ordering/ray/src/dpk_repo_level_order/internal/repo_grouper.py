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

import concurrent.futures
from functools import partial
import multiprocessing
import os
import threading
from typing import List

import concurrent
import pandas as pd
import pyarrow as pa
import ray
from data_processing.utils import get_logger


def _read_table_for_group(file, data_access=None, grouping_column=None, group=None):
    logger = get_logger(__name__)
    logger.info(f"[{threading.current_thread().name}] start reading table for group")
    table, _ = data_access.get_table(os.path.normpath(file))
    # filtering each table is more memory efficient than
    # reading all tables and filtering later.
    column_data = table.column(grouping_column)
    row_mask = pa.compute.equal(column_data, group)
    filtered_table = table.filter(row_mask)
    logger.info(f"[{threading.current_thread().name}] end reading table for group")
    return filtered_table.to_pandas()

def _async_read_table_for_group(files, data_access=None, grouping_column=None, group=None):
    """This function reads the files and filters the tables based on grouping_column value"""
    func = partial(_read_table_for_group, data_access=data_access, grouping_column=grouping_column, group=group)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(_read_table_for_group, file, data_access, grouping_column, group) for file in files]
    return pd.concat([future.result() for future in concurrent.futures.as_completed(futures)])


class GroupByRepo:
    """
    This class can read a list of parquet files and transform the rows of
    `repo_column_name` and apply the user provided mapper function to them
    and write the result to local disk or s3 bucket.

    """

    def __init__(
        self,
        repo_column_name,
        output_dir,
        logger,
        data_access,
        table_mapper=None,
        n_processes=1,
    ):
        self.repo_column_name = repo_column_name
        self.output_dir = output_dir
        self.logger = get_logger(__name__)
        self.data_access = data_access
        self.enable_superrows = True
        self.table_mapper = table_mapper
        self.n_processes = n_processes
        if self.table_mapper is None:
            """
            table_mapper is a function of signature: func(table: pa.Table, filename: str)-> List[Tuple[pa.Table, filename]]:
            """
            self.table_mapper = self._default_mapper_func

    def _default_mapper_func(self, table, file_name):
        return [
            (table, file_name),
        ]

    def process(self, repo: str, files: List[str]):
        try:
            func = partial(_async_read_table_for_group, data_access=self.data_access, grouping_column=self.repo_column_name, group=repo)
            with multiprocessing.get_context("spawn").Pool(processes=self.n_processes, maxtasksperchild=None) as pool:
                results = list(
                    pool.imap(
                        func,
                        self._chunks(files),
                    )
                )
            df = pd.concat(results)
            repo_table = pa.Table.from_pandas(df)

            if len(repo_table) == 0:
                # not processing empty table
                return

            def sanitize_path(repo_name):
                return repo_name.replace("/", "%2F")

            repo = sanitize_path(repo)
            tables = self.table_mapper(repo_table, repo)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                concurrent.futures.wait(
                    [executor.submit(self._write_parquet, out_table, filename) for out_table, filename in tables]
                )
        except Exception as e:
            self.logger.error(f"Failed processing repo: {repo}. {e}")

    def _chunks(self, files):
        n = len(files)
        chunk_size = n // self.n_processes

        chunks: list[list[str]] = []
        i = 0
        while i < self.n_processes:
            if i == self.n_processes - 1:
                remainder = n % self.n_processes
            else:
                remainder = 0
            chunk = files[i * chunk_size : i * chunk_size + chunk_size + remainder]
            chunks.append(list(chunk))
            i += 1

        return chunks

    def _write_parquet(self, table, repo_name):
        self.logger.info(f"Write {repo_name}, tables: {len(table)}")
        # since we already know the repo
        # self.output_path should have the basepath where to write
        parquet_path = os.path.join(self.output_dir, f"{repo_name}.parquet")
        self.data_access.save_table(os.path.normpath(parquet_path), table)

    def _read_table_for_group(self, grouping_column, group, files):
        """This function reads the files and filters the tables based on grouping_column value"""
        dfs = []
        for file in files:
            table, _ = self.data_access.get_table(os.path.normpath(file))
            # filtering each table is more memory efficient than
            # reading all tables and filtering later.
            filtered_table = self._filter_table_by_column(table, grouping_column, group)
            dfs.append(filtered_table.to_pandas())
        df = pd.concat(dfs)

        return pa.Table.from_pandas(df)

    def _filter_table_by_column(self, table: pa.Table, column_name: str, column_value: str) -> pa.Table:
        """
        Filters rows in a PyArrow table based on the specified column value.

        Args:
            table (pa.Table): The input PyArrow table.
            column_name (str): The name of the column to filter.
            column_value (str): The value to match in the specified column.

        Returns:
            pa.Table: A new table containing only rows where the specified column has the given value.
        """

        column_data = table.column(column_name)
        row_mask = pa.compute.equal(column_data, column_value)
        filtered_table = table.filter(row_mask)

        return filtered_table


@ray.remote(scheduling_strategy="SPREAD")
class GroupByRepoActor(GroupByRepo):
    """
    This Actor represents a proxy to the class `GroupByRepo`.

    A sample `params` dict for this actor looks like.

      params = {
         'repo_column_name': str,
         'output_dir': str,
         'mapper': A function with signature: func(table: pa.Table, filename: str)->List[Tuple[pa.Table, str]]
         'data_access_creds': A dict os s3 creds or None eg. {'access_key': <>, 'secret_key': <>, 'url': <>}
      }

    """

    def __init__(self, params: dict):
        super().__init__(
            params["repo_column_name"],
            params["output_dir"],
            None,
            params["data_access_factory"].create_data_access(),
            params["mapper"],
            params["n_processes"]
        )
