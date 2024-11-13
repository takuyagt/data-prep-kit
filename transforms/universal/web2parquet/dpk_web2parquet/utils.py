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

