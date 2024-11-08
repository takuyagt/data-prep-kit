from datetime import datetime

def get_file_info(headers, url=None):
    # Extract file size
    file_size = int(headers.get('Content-Length', 0))  # Default to 0 if not found

    # Extract filename from Content-Disposition
    try:
        filename = headers.get('Content-Disposition').split('filename=')[-1].strip().strip('"')
    except:
        filename = None

    # try to find the file name from
    if not filename:
        try:
            parts = url.split('/')
            filename = parts[-1]
        except:
            filename= url
    return filename, headers.get('Content-Type') 

