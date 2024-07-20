"""
Test generic functions
"""

import itertools
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from rwkit.docx import read_docx, write_docx
from rwkit.generic import read, write
from rwkit.json import read_json, read_jsonl, write_json, write_jsonl
from rwkit.text import read_text, write_text
from rwkit.yaml import read_yaml, write_yaml


class TestGeneric(unittest.TestCase):
    """TestGeneric"""

    def test_read(self):
        """test_read"""

        object_expected_list = [
            {"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None},
            [{"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None}],
            0,
            0.1,
            True,
            False,
            None,
            "This is a test",
            ["This is a test"],
            ["This is a test", "This is another test"],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 3, "A", "b", "c", True, False, None],
        ]
        file_type_list = ["text", "json", "jsonl", "yaml", "docx"]
        add_file_type_list = [True, False]
        infer_file_type_list = [True, False]
        compression_list = (
            None,
            "bz2",
            "gzip",
            "tar",
            "tar.bz2",
            "tar.gz",
            "tgz",
            "tar.xz",
            "xz",
            "zip",
            "zstd",
        )
        add_compression_suffix_list = [True, False]
        infer_compression_list = [True, False]

        for (
            object_expected,
            file_type,
            add_file_type,
            infer_file_type,
            compression,
            add_compression_suffix,
            infer_compression,
        ) in itertools.product(
            object_expected_list,
            file_type_list,
            add_file_type_list,
            infer_file_type_list,
            compression_list,
            add_compression_suffix_list,
            infer_compression_list,
        ):
            with TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir) / "file"

                applicable_to = ("json", "jsonl", "yaml")
                if isinstance(object_expected, str):
                    applicable_to += ("text", "docx")
                elif isinstance(object_expected, list):
                    if isinstance(object_expected[0], str):
                        applicable_to += ("docx",)

                if file_type not in applicable_to:
                    continue

                if add_file_type:
                    if file_type == "text":
                        filepath = filepath.parent / (filepath.name + ".txt")
                    elif file_type == "json":
                        filepath = filepath.parent / (filepath.name + ".json")
                    elif file_type == "jsonl":
                        filepath = filepath.parent / (filepath.name + ".jsonl")
                    elif file_type == "yaml":
                        filepath = filepath.parent / (filepath.name + ".yaml")
                    elif file_type == "docx":
                        filepath = filepath.parent / (filepath.name + ".docx")
                    else:
                        raise ValueError("Unsupported file type: %s" % file_type)

                if add_compression_suffix:
                    if compression is None:
                        pass
                    elif compression == "gzip":
                        filepath = filepath.parent / (filepath.name + ".gz")
                    elif compression == "bz2":
                        filepath = filepath.parent / (filepath.name + ".bz2")
                    elif compression.startswith("tar") | (compression == "tgz"):
                        filepath = filepath.parent / (filepath.name + "." + compression)
                    elif compression == "xz":
                        filepath = filepath.parent / (filepath.name + ".xz")
                    elif compression == "zip":
                        filepath = filepath.parent / (filepath.name + ".zip")
                    elif compression == "zstd":
                        filepath = filepath.parent / (filepath.name + ".zst")

                write_kwargs = {}
                if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                    if add_compression_suffix:
                        write_kwargs["compression"] = "infer"
                    else:
                        write_kwargs["compression"] = "tar"
                        if "." in compression:
                            write_kwargs["mode"] = "w:" + compression.split(".")[1]
                        else:
                            write_kwargs["mode"] = "w:gz"
                else:
                    write_kwargs["compression"] = compression

                # Write to file
                if file_type == "text":
                    write_text(filename=filepath, text=object_expected, **write_kwargs)
                elif file_type == "json":
                    write_json(
                        filename=filepath, object=object_expected, **write_kwargs
                    )
                elif file_type == "jsonl":
                    write_jsonl(
                        filename=filepath, object=object_expected, **write_kwargs
                    )
                elif file_type == "yaml":
                    write_yaml(
                        filename=filepath, object=object_expected, **write_kwargs
                    )
                elif file_type == "docx":
                    if compression is not None:
                        # docx does not support compression
                        continue
                    write_docx(filename=filepath, text=object_expected)
                else:
                    raise ValueError("Unsupported file type: %s" % file_type)

                # Read file contents
                read_file_type = (
                    "infer" if infer_file_type & add_file_type else file_type
                )
                read_compression = (
                    "infer"
                    if infer_compression & add_compression_suffix
                    else compression
                )

                read_kwargs = {}
                if file_type == "docx":
                    pass
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if add_compression_suffix:
                            read_kwargs["compression"] = "infer"
                        else:
                            read_kwargs["compression"] = "tar"
                            if "." in compression:
                                read_kwargs["mode"] = "r:" + compression.split(".")[1]
                            else:
                                read_kwargs["mode"] = "r:gz"
                    else:
                        read_kwargs["compression"] = compression

                object_observed = read(
                    filename=filepath, file_type=read_file_type, **read_kwargs
                )

                if file_type in ["jsonl", "docx"]:
                    if not isinstance(object_expected, list):
                        object_observed = object_observed[0]

                self.assertEqual(
                    object_expected,
                    object_observed,
                    "read() failed.\n"
                    "Parameters:\n"
                    f"  file_type: {file_type}\n"
                    f"  read_file_type: {read_file_type}\n"
                    f"  add_file_type: {add_file_type}\n"
                    f"  infer_file_type: {infer_file_type}\n"
                    f"  compression: {compression}\n"
                    f"  read_compression: {read_compression}\n"
                    f"  add_compression_suffix: {add_compression_suffix}\n"
                    f"  infer_compression: {infer_compression}\n"
                    f"Expected: '{object_expected}'\n"
                    f"Observed: '{object_observed}'",
                )

    def test_write(self):
        """test_write"""

        object_expected_list = [
            {"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None},
            [{"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None}],
            0,
            0.1,
            True,
            False,
            None,
            "This is a test",
            ["This is a test"],
            ["This is a test", "This is another test"],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 3, "A", "b", "c", True, False, None],
        ]
        file_type_list = ["text", "json", "jsonl", "yaml", "docx"]
        add_file_type_list = [True, False]
        infer_file_type_list = [True, False]
        compression_list = (
            None,
            "bz2",
            "gzip",
            "tar",
            "tar.bz2",
            "tar.gz",
            "tgz",
            "tar.xz",
            "xz",
            "zip",
            "zstd",
        )
        add_compression_suffix_list = [True, False]
        infer_compression_list = [True, False]

        for (
            object_expected,
            file_type,
            add_file_type,
            infer_file_type,
            compression,
            add_compression_suffix,
            infer_compression,
        ) in itertools.product(
            object_expected_list,
            file_type_list,
            add_file_type_list,
            infer_file_type_list,
            compression_list,
            add_compression_suffix_list,
            infer_compression_list,
        ):
            with TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir) / "file"

                applicable_to = ("json", "jsonl", "yaml")
                if isinstance(object_expected, str):
                    applicable_to += ("text", "docx")
                elif isinstance(object_expected, list):
                    if isinstance(object_expected[0], str):
                        applicable_to += ("docx",)

                if file_type not in applicable_to:
                    continue

                if add_file_type:
                    if file_type == "text":
                        filepath = filepath.parent / (filepath.name + ".txt")
                    elif file_type == "json":
                        filepath = filepath.parent / (filepath.name + ".json")
                    elif file_type == "jsonl":
                        filepath = filepath.parent / (filepath.name + ".jsonl")
                    elif file_type == "yaml":
                        filepath = filepath.parent / (filepath.name + ".yaml")
                    elif file_type == "docx":
                        filepath = filepath.parent / (filepath.name + ".docx")
                    else:
                        raise ValueError("Unsupported file type: %s" % file_type)

                if add_compression_suffix:
                    if compression is None:
                        pass
                    elif compression == "gzip":
                        filepath = filepath.parent / (filepath.name + ".gz")
                    elif compression == "bz2":
                        filepath = filepath.parent / (filepath.name + ".bz2")
                    elif compression.startswith("tar") | (compression == "tgz"):
                        filepath = filepath.parent / (filepath.name + "." + compression)
                    elif compression == "xz":
                        filepath = filepath.parent / (filepath.name + ".xz")
                    elif compression == "zip":
                        filepath = filepath.parent / (filepath.name + ".zip")
                    elif compression == "zstd":
                        filepath = filepath.parent / (filepath.name + ".zst")

                write_kwargs = {}
                if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                    if add_compression_suffix:
                        write_kwargs["compression"] = "infer"
                    else:
                        write_kwargs["compression"] = "tar"
                        if "." in compression:
                            write_kwargs["mode"] = "w:" + compression.split(".")[1]
                        else:
                            write_kwargs["mode"] = "w:gz"
                elif file_type != "docx":
                    write_kwargs["compression"] = compression

                if (file_type == "docx") & (compression is not None):
                    # docx does not support compression
                    continue

                # Write to file
                write(
                    filename=filepath,
                    text_or_object=object_expected,
                    file_type=file_type,
                    **write_kwargs,
                )

                # Read file contents
                read_file_type = (
                    "infer" if infer_file_type & add_file_type else file_type
                )
                read_compression = (
                    "infer"
                    if infer_compression & add_compression_suffix
                    else compression
                )

                read_kwargs = {}
                if file_type == "docx":
                    pass
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if add_compression_suffix:
                            read_kwargs["compression"] = "infer"
                        else:
                            read_kwargs["compression"] = "tar"
                            if "." in compression:
                                read_kwargs["mode"] = "r:" + compression.split(".")[1]
                            else:
                                read_kwargs["mode"] = "r:gz"
                    else:
                        read_kwargs["compression"] = compression

                if file_type == "text":
                    object_observed = read_text(filename=filepath, **read_kwargs)
                elif file_type == "json":
                    object_observed = read_json(filename=filepath, **read_kwargs)
                elif file_type == "jsonl":
                    object_observed = read_jsonl(filename=filepath, **read_kwargs)
                elif file_type == "yaml":
                    object_observed = read_yaml(filename=filepath, **read_kwargs)
                elif file_type == "docx":
                    object_observed = read_docx(filename=filepath, **read_kwargs)
                else:
                    raise ValueError("Unsupported file type: %s" % file_type)

                if file_type in ["jsonl", "docx"]:
                    if not isinstance(object_expected, list):
                        object_observed = object_observed[0]

                self.assertEqual(
                    object_expected,
                    object_observed,
                    "read() failed.\n"
                    "Parameters:\n"
                    f"  file_type: {file_type}\n"
                    f"  read_file_type: {read_file_type}\n"
                    f"  add_file_type: {add_file_type}\n"
                    f"  infer_file_type: {infer_file_type}\n"
                    f"  compression: {compression}\n"
                    f"  read_compression: {read_compression}\n"
                    f"  add_compression_suffix: {add_compression_suffix}\n"
                    f"  infer_compression: {infer_compression}\n"
                    f"Expected: '{object_expected}'\n"
                    f"Observed: '{object_observed}'",
                )

    def test_read_write(self):
        """test_read_write"""

        object_expected_list = [
            {"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None},
            [{"A": "a", "B": 1, "C": 0.1, "D": True, "E": False, "F": None}],
            0,
            0.1,
            True,
            False,
            None,
            "This is a test",
            ["This is a test"],
            ["This is a test", "This is another test"],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 3, "A", "b", "c", True, False, None],
        ]
        file_type_list = ["text", "json", "jsonl", "yaml", "docx"]
        add_file_type_list = [True, False]
        infer_file_type_list = [True, False]
        compression_list = (
            None,
            "bz2",
            "gzip",
            "tar",
            "tar.bz2",
            "tar.gz",
            "tgz",
            "tar.xz",
            "xz",
            "zip",
            "zstd",
        )
        add_compression_suffix_list = [True, False]
        infer_compression_list = [True, False]

        for (
            object_expected,
            file_type,
            add_file_type,
            infer_file_type,
            compression,
            add_compression_suffix,
            infer_compression,
        ) in itertools.product(
            object_expected_list,
            file_type_list,
            add_file_type_list,
            infer_file_type_list,
            compression_list,
            add_compression_suffix_list,
            infer_compression_list,
        ):
            with TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir) / "file"

                applicable_to = ("json", "jsonl", "yaml")
                if isinstance(object_expected, str):
                    applicable_to += ("text", "docx")
                elif isinstance(object_expected, list):
                    if isinstance(object_expected[0], str):
                        applicable_to += ("docx",)

                if file_type not in applicable_to:
                    continue

                if add_file_type:
                    if file_type == "text":
                        filepath = filepath.parent / (filepath.name + ".txt")
                    elif file_type == "json":
                        filepath = filepath.parent / (filepath.name + ".json")
                    elif file_type == "jsonl":
                        filepath = filepath.parent / (filepath.name + ".jsonl")
                    elif file_type == "yaml":
                        filepath = filepath.parent / (filepath.name + ".yaml")
                    elif file_type == "docx":
                        filepath = filepath.parent / (filepath.name + ".docx")
                    else:
                        raise ValueError("Unsupported file type: %s" % file_type)

                if add_compression_suffix:
                    if compression is None:
                        pass
                    elif compression == "gzip":
                        filepath = filepath.parent / (filepath.name + ".gz")
                    elif compression == "bz2":
                        filepath = filepath.parent / (filepath.name + ".bz2")
                    elif compression.startswith("tar") | (compression == "tgz"):
                        filepath = filepath.parent / (filepath.name + "." + compression)
                    elif compression == "xz":
                        filepath = filepath.parent / (filepath.name + ".xz")
                    elif compression == "zip":
                        filepath = filepath.parent / (filepath.name + ".zip")
                    elif compression == "zstd":
                        filepath = filepath.parent / (filepath.name + ".zst")

                write_kwargs = {}
                if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                    if add_compression_suffix:
                        write_kwargs["compression"] = "infer"
                    else:
                        write_kwargs["compression"] = "tar"
                        if "." in compression:
                            write_kwargs["mode"] = "w:" + compression.split(".")[1]
                        else:
                            write_kwargs["mode"] = "w:gz"
                elif file_type != "docx":
                    write_kwargs["compression"] = compression

                if (file_type == "docx") & (compression is not None):
                    # docx does not support compression
                    continue

                # Write to file
                write(
                    filename=filepath,
                    text_or_object=object_expected,
                    file_type=file_type,
                    **write_kwargs,
                )

                # Read file contents
                read_file_type = (
                    "infer" if infer_file_type & add_file_type else file_type
                )
                read_compression = (
                    "infer"
                    if infer_compression & add_compression_suffix
                    else compression
                )

                read_kwargs = {}
                if file_type == "docx":
                    pass
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if add_compression_suffix:
                            read_kwargs["compression"] = "infer"
                        else:
                            read_kwargs["compression"] = "tar"
                            if "." in compression:
                                read_kwargs["mode"] = "r:" + compression.split(".")[1]
                            else:
                                read_kwargs["mode"] = "r:gz"
                    else:
                        read_kwargs["compression"] = compression

                object_observed = read(
                    filename=filepath, file_type=read_file_type, **read_kwargs
                )

                if file_type in ["jsonl", "docx"]:
                    if not isinstance(object_expected, list):
                        object_observed = object_observed[0]

                self.assertEqual(
                    object_expected,
                    object_observed,
                    "read() failed.\n"
                    "Parameters:\n"
                    f"  file_type: {file_type}\n"
                    f"  read_file_type: {read_file_type}\n"
                    f"  add_file_type: {add_file_type}\n"
                    f"  infer_file_type: {infer_file_type}\n"
                    f"  compression: {compression}\n"
                    f"  read_compression: {read_compression}\n"
                    f"  add_compression_suffix: {add_compression_suffix}\n"
                    f"  infer_compression: {infer_compression}\n"
                    f"Expected: '{object_expected}'\n"
                    f"Observed: '{object_observed}'",
                )
