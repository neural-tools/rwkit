# rwkit

`rwkit` is a Python package that simplifies reading and writing various file formats, including text, json, jsonl and yaml. It supports transparent handling of compression, and allows for processing large files in chunks.

## Features

-   Easy-to-use functions for reading and writing text, json, jsonl and yaml files.
-   Transparent compression support: bz2, gzip, tar, tar.bz2, tar.gz, tar.xz, xz, zip, zstd.
-   Generator functions for processing large files in chunks.

## Installation

Install `rwkit` using pip:

```bash
pip install rwkit
```

## Quick Start

Here are some examples to get you started:

### Reading and Writing Text Files

Using a single string:

```python
import rwkit as rw


# Sample text
text = "Hello, rwkit!"

# Write a string
rw.write_text("file.txt", text)

# Append another string
rw.write_text("file.txt", "\nNice to meet you.", mode="a")

# Read file
loaded_text = rw.read_text("file.txt")

print(loaded_text)
# Output: 'Hello, rwkit!\nNice to meet you.'
```

... using lines (= list of strings):

```python
import rwkit as rw


# Sample
lines = ["Hello, rwkit!", "Nice to meet you."]

# Write lines, each element on its own line (separated by '\n')
rw.write_lines("file.txt", lines)

# Append a line(s)
rw.write_lines("file.txt", "What a beautiful day.", mode="a")

# Read file (transparently splits on '\n')
loaded_lines = rw.read_lines("file.txt")

print(loaded_lines)
# Output: ['Hello, rwkit!', 'Nice to meet you.', 'What a beautiful day.']
```

### Reading and Writing JSON Files

Using a single object:

```python
import rwkit as rw


# Sample data
data = {"name": "Alice", "age": 25}

# Write data to a JSON file
rw.write_json("file.json", data)

# Read data
loaded_data = rw.read_json("file.json")

print(loaded_data)
# Output: {'name': 'Alice', 'age': 25}
```

### Reading and Writing JSONL (= JSON Lines) Files

Using multiple objects, each on their own line. This format is especially useful for large files that are processed in chunks (see also below).

```python
import rwkit as rw


# Sample data
data = [
    {"name": "Alice", "age": 25},
    {"name": "Bob", "age": 30},
]

# Write data to a JSONL file
rw.write_jsonl("file.jsonl", data)

# Read data
loaded_data = rw.read_jsonl("file.jsonl")

print(loaded_data)
# Output: [{'name': 'Alice', 'age': 25}, {'name': 'Bob', 'age': 30}]
```

### Reading and Writing YAML Files

Note: Requires `pyyaml` package.

```python
import rwkit as rw


# Sample data
data = {"name": "Alice", "age": 25}

# Write to a YAML file
rw.write_yaml("file.yaml", data)

# Read a YAML file
loaded_data = rw.read_yaml("file.yaml")

print(loaded_data)
# Output: {'name': 'Alice', 'age': 25}
```

## Compression

`rwkit` supports various compression formats via argument `compression`. The default is `compression='infer'`, which tries to infer it from the filename extension:

```python
import rwkit as rw


# Sample text
text = "Hello, rwkit!"

# Write to a gzip compressed text file, inferred from the filename extension
rw.write_text("file.txt.gz", text)

# Read a gzip compressed text file
loaded_text = rw.read_text("file.txt.gz")

print(loaded_text)
# Output: 'Hello, rwkit!'
```

Alternatively, specify `compression` explicitly (see all available options in table
below):

```python
import rwkit as rw


# Sample text
text = "Hello, rwkit!"

# Write to a gzip compressed text file, explicitly specified
rw.write_text("file.txt.gz", text, compression="gzip")

# Read a gzip compressed text file, explicitly specified
loaded_text = rw.read_text("file.txt.gz", compression="gzip")

print(loaded_text)
# Output: 'Hello, rwkit!'
```

When `compression='infer'`, the following rules apply:

| File extension    | Inferred compression |
| ----------------- | -------------------- |
| `.tar`            | `tar`                |
| `.tar.bz2`        | `tar.bz2`            |
| `.tar.gz`         | `tar.gz`             |
| `.tar.xz`         | `tar.xz`             |
| `.bz2`            | `bz2`                |
| `.gz`             | `gzip`               |
| `.xz`             | `xz`                 |
| `.zip`            | `zip`                |
| `.zst`            | `zstd`               |
| [everything else] | None                 |

## Reading Large Files in Chunks

Both text and jsonl files can be read in chunks using the `chunksize` argument. This
also works in combination with `compression`.

```python
import rwkit as rw


# Assume a large text file, optionally compressed
for chunk in rw.read_lines("file.txt", chunksize=3):
    print(chunk)
    # Output: ['Hello, rwkit!', 'Nice to meet you.', 'What a beautiful day.']
    # ...

# The same works for jsonl files
for chunk in rw.read_jsonl("file.jsonl", chunksize=3):
    print(chunk)
    # Output: [{'name': 'Alice'}, {'name': 'Bob'}, {'name': 'Charlie'}]
    # ...
```

## License

`rwkit` is released under the Apache License Version 2.0. See the LICENSE file for details.
