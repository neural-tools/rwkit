# rwkit

`rwkit` is a Python package that simplifies reading and writing various file formats, including text, json, yaml, and docx. It supports transparent handling of compressed files, and also provides generator functions for processing large files in chunks.

## Features

-   Easy-to-use functions for reading and writing text, json, yaml, and docx files.
-   Transparent compression support: bz2, gzip, tar, tar.bz2, tar.gz, tar.xz, xz, zip, zstd
-   Generator functions for processing large files in chunks

## Installation

Install `rwkit` using pip:

```bash
pip install rwkit
```

## Quick Start

Here are some examples to get you started:

### Reading and Writing Text Files

```python
import rwkit as rw

# Write to a text file
rw.write_text("path/to/file.txt", "Hello, rwkit!\nNice to meet you.")
# Alternatively, use:
#   rw.write_text("path/to/file.txt", ["Hello, rwkit!", "Nice to meet you."])

# Read a text file
text = rw.read_text("path/to/file.txt")
# 'Hello, rwkit!\nNice to meet you.'

# Read a text file as list of lines
text = rw.read_text("path/to/file.txt", lines=True)
# ['Hello, rwkit!', 'Nice to meet you.']
```

It also supports appending to a file:

```python
import rwkit as rw

rw.write_text("path/to/file.txt", "Hello, rwkit!")
# File content: 'Hello, rwkit!'

rw.write_text("path/to/file.txt", " Nice to meet you.", mode="a")
# File content: 'Hello, rwkit! Nice to meet you.'
```

### Reading and Writing JSON Files

```python
import rwkit as rw

# Write to a JSON file
rw.write_json("path/to/file.json", {"key": "value"})

# Read a JSON file
data = rw.read_json("path/to/file.json")
# {"key": "value"}
```

### Reading and Writing JSONL (= JSON Lines) Files

```python
import rwkit as rw

# Write to a JSONL file
rw.write_jsonl("path/to/file.jsonl", [{"key": "value"}])

# Read a JSON file
data = rw.read_json("path/to/file.json")
# {"key": "value"}
```

### Reading and Writing YAML Files

```python
import rwkit as rw

# Write to a YAML file
rw.write_yaml("path/to/file.yaml", {"key": "value"})

# Read a YAML file
data = rw.read_yaml("path/to/file.yaml")
# {"key": "value"}
```

### Reading and Writing Docx Files

```python
import rwkit as rw

# Write to a Docx file
rw.write_docx("path/to/file.docx", "Hello, rwkit!")

# Read a Docx file
text = rw.read_docx("path/to/file.docx")
# ['Hello, rwkit!']
```

## Compression

`rwkit` supports compression formats bz2, gzip, tar, tar.bz2, tar.gz, tar.xz, xz, zip and zstd. By default, `rwkit` infers the format from the filename extension:

```python
import rwkit as rw

# Write to a gzip compressed text file, inferred from the filename extension
rw.write_text("path/to/file.txt.gz", "Hello, rwkit!")

# Read a gzip compressed text file
text = rw.read_text("path/to/file.txt.gz")
# 'Hello, rwkit!'
```

Specify the format using the `compression` argument:

```python
import rwkit as rw

# Write to a gzip compressed text file, explicitly specified
rw.write_text("path/to/file.txt.gz", "Hello, rwkit!", compression="gzip")

# Read a gzip compressed text file
text = rw.read_text("path/to/file.txt.gz", compression="gzip")
# 'Hello, rwkit!'
```

## Reading Large Files in Chunks

Both text and json files can be read in chunks using the `chunksize` argument:

```python
import rwkit as rw

# Assume a text file with 10,000 lines
for chunk in rw.read_text("path/to/file.txt", chunksize=3)
    print(chunk)
    # [line 1, line 2, line 3]
    # [line 4, line 5, line 6]
    # ...

# The same is supported for jsonl (= JSON lines) files
for chunk in rw.read_json("path/to/file.json", lines=True, chunksize=3)
    print(chunk)
    # [{'key_line_1': 'value'}, {'key_line_2': 'value'}, {'key_line_3': 'value'}]
    # [{'key_line_4': 'value'}, {'key_line_5': 'value'}, {'key_line_6': 'value'}]
    # ...
```

## License

`rwkit` is released under the Apache License Version 2.0. See the LICENSE file for details.
