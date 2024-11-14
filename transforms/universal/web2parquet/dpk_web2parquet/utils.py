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

from datetime import datetime

def get_file_info(headers, url):
    # Extract file size
    file_size = int(headers.get('Content-Length', 0))  # Default to 0 if not found
    content_type = headers.get('Content-Type')
    try:
        filename = headers.get('Content-Disposition').split('filename=')[1].strip().strip('"')
    except:
        url_split=url.split('/')
        filename = url_split[-1] if not url.endswith('/') else url_split[-2]
        filename = filename.replace('.','_')+"-"+content_type.replace("/", ".")

    return filename, content_type, file_size

