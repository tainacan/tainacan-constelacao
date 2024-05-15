import re


def clean_file_name(file_name):
    # Remove special characters and dots
    cleaned_name = re.sub(r'[^\w\-.]', '_', file_name)
    # Split file name from extension
    parts = cleaned_name.split('.')
    # Get only the part without extension
    name_without_extension = parts[0]
    return name_without_extension
