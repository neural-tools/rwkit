"""
Test YAML file I/O
"""

import bz2
import gzip
import itertools
import lzma
import tarfile
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml
import zstandard

from rwkit.io_yaml import read_yaml, write_yaml


class TestYaml(unittest.TestCase):
    """TestYaml"""

    def test_read_yaml(self):
        """test_read_yaml"""

        data_expected = {"A": 1, "B": -0.1, "C": "c", "D": None}
        data_expected_serialized = yaml.dump(data_expected, sort_keys=False)
        data_expected_serialized_binary = data_expected_serialized.encode()

        add_file_extension_list = [True, False]
        infer_list = [True, False]
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

        for add_file_extension, compression, infer in itertools.product(
            add_file_extension_list, compression_list, infer_list
        ):
            with TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir) / "file"

                # Write to file
                if compression is None:
                    with open(filepath, mode="w") as handle:
                        yaml.dump(data_expected, stream=handle, sort_keys=False)
                elif compression == "bz2":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".bz2")
                    with bz2.open(filepath, mode="wb") as handle:
                        handle.write(data_expected_serialized_binary)
                elif compression == "gzip":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".gz")
                    with gzip.open(filepath, mode="wb") as handle:
                        handle.write(data_expected_serialized_binary)
                elif compression == "xz":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".xz")
                    with lzma.open(
                        filepath, format=lzma.FORMAT_XZ, mode="wb"
                    ) as handle:
                        handle.write(data_expected_serialized_binary)
                elif compression == "zip":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".zip")
                    with zipfile.ZipFile(filepath, mode="w") as container_handle:
                        with container_handle.open("data", mode="w") as file_handle:
                            file_handle.write(data_expected_serialized_binary)
                elif compression in ("tar", "tar.bz2", "tar.gz", "tgz", "tar.xz"):
                    if add_file_extension:
                        filepath = filepath.with_suffix("." + compression)

                    tar_mode = "w"
                    if compression in ("tar.bz2", "tar.gz", "tar.xz"):
                        tar_mode += ":" + compression.split(".")[1]
                    elif compression == "tgz":
                        tar_mode += ":gz"

                    with tarfile.open(filepath, mode=tar_mode) as container_handle:
                        tar_info = tarfile.TarInfo(name="data")
                        tar_info.size = len(data_expected_serialized_binary)
                        container_handle.addfile(
                            tar_info,
                            fileobj=BytesIO(data_expected_serialized_binary),
                        )
                elif compression == "zstd":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".zst")
                    with zstandard.open(filepath, mode="w") as handle:
                        handle.write(data_expected_serialized)
                else:
                    raise NotImplementedError(compression)

                # Read file contents
                if infer & ((compression is None) | add_file_extension):
                    data_observed = read_yaml(
                        filename=filepath,
                        mode="r",
                        compression="infer",
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if "." in compression:
                            mode = "r:" + compression.split(".")[1]
                        else:
                            mode = "r:gz"

                        data_observed = read_yaml(
                            filename=filepath,
                            mode=mode,
                            compression="tar",
                        )
                    else:
                        data_observed = read_yaml(
                            filename=filepath,
                            mode="r",
                            compression=compression,
                        )

                self.assertEqual(
                    data_expected,
                    data_observed,
                    "read_yaml() failed.\n"
                    "Parameters:\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{data_expected}'\n"
                    f"Observed: '{data_observed}'",
                )

    def test_write_yaml(self):
        """test_write_yaml"""

        # Unknown compression raises NotImplementedError
        with TemporaryDirectory() as tmpdir:
            filename = Path(tmpdir) / "file"
            self.assertRaises(
                ValueError,
                write_yaml,
                filename=filename,
                data="{}",
                mode="w",
                compression="?",
            )

        data_expected = {"A": 1, "B": -0.1, "C": "c", "D": None}

        add_file_extension_list = [True, False]
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
            "?",
        )
        infer_list = [True, False]

        for add_file_extension, compression, infer in itertools.product(
            add_file_extension_list, compression_list, infer_list
        ):
            with TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir) / "file"

                if add_file_extension:
                    if compression is None:
                        pass
                    elif compression == "gzip":
                        filepath = filepath.with_suffix(".gz")
                    elif compression == "bz2":
                        filepath = filepath.with_suffix(".bz2")
                    elif compression.startswith("tar") | (compression == "tgz"):
                        filepath = filepath.with_suffix("." + compression)
                    elif compression == "xz":
                        filepath = filepath.with_suffix(".xz")
                    elif compression == "zip":
                        filepath = filepath.with_suffix(".zip")
                    elif compression == "zstd":
                        filepath = filepath.with_suffix(".zst")

                if compression == "?":
                    self.assertRaises(
                        ValueError,
                        write_yaml,
                        filename=filepath,
                        data=data_expected,
                        mode="w",
                        compression="?",
                        level=None,
                    )
                    continue

                # Write to new file
                mode = "w"
                if compression in ("tar.bz2", "tar.gz", "tar.xz"):
                    mode += ":" + compression.split(".")[1]
                elif compression == "tgz":
                    mode += ":gz"

                if infer & ((compression is None) | add_file_extension):
                    write_yaml(
                        filename=filepath,
                        data=data_expected,
                        mode=mode,
                        compression="infer",
                        level=None,
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        write_yaml(
                            filename=filepath,
                            data=data_expected,
                            mode=mode,
                            compression="tar",
                            level=None,
                        )
                    else:
                        write_yaml(
                            filename=filepath,
                            data=data_expected,
                            mode=mode,
                            compression=compression,
                            level=None,
                        )

                # Read file contents
                if compression is None:
                    with open(filepath, mode="r") as handle:
                        content = handle.read()
                elif compression == "bz2":
                    with bz2.open(filepath, mode="r") as handle:
                        content = handle.read().decode()
                elif compression == "gzip":
                    with gzip.open(filepath, mode="r") as handle:
                        content = handle.read().decode()
                elif compression.startswith("tar") | (compression == "tgz"):
                    with tarfile.open(filepath, mode="r") as container_handle:
                        file_list = container_handle.getnames()
                        self.assertEqual(
                            len(file_list),
                            1,
                            "tar archive must contain exactly 1 file.",
                        )
                        file_in_archive = file_list[0]

                        with container_handle.extractfile(
                            file_in_archive
                        ) as file_handle:
                            content = file_handle.read().decode()
                elif compression == "xz":
                    with lzma.open(filepath, format=lzma.FORMAT_XZ, mode="r") as handle:
                        content = handle.read().decode()
                elif compression == "zip":
                    with zipfile.ZipFile(filepath, mode="r") as container_handle:
                        file_list = container_handle.namelist()
                        self.assertEqual(
                            len(file_list),
                            1,
                            "zip archive must contain exactly 1 file.",
                        )
                        file_in_archive = file_list[0]

                        with container_handle.open(
                            file_in_archive, mode="r"
                        ) as file_handle:
                            content = file_handle.read().decode()
                elif compression == "zstd":
                    with zstandard.open(filepath, mode="r") as handle:
                        content = handle.read()
                else:
                    raise NotImplementedError(compression)

                data_observed = yaml.safe_load(content)

                self.assertEqual(
                    data_expected,
                    data_observed,
                    "write_yaml() failed.\n"
                    "Parameters:\n"
                    f"  mode: 'w'\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{data_expected}'\n"
                    f"Observed: '{data_observed}'",
                )

    def test_read_write_yaml(self):
        """test_read_write_yaml"""

        data_expected = {"A": 1, "B": -0.1, "C": "c", "D": None}

        add_file_extension_list = [True, False]
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
            "?",
        )
        infer_list = [True, False]

        for add_file_extension, compression, infer in itertools.product(
            add_file_extension_list, compression_list, infer_list
        ):
            with TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir) / "file"

                if add_file_extension:
                    if compression is None:
                        pass
                    elif compression == "gzip":
                        filepath = filepath.with_suffix(".gz")
                    elif compression == "bz2":
                        filepath = filepath.with_suffix(".bz2")
                    elif compression.startswith("tar") | (compression == "tgz"):
                        filepath = filepath.with_suffix("." + compression)
                    elif compression == "xz":
                        filepath = filepath.with_suffix(".xz")
                    elif compression == "zip":
                        filepath = filepath.with_suffix(".zip")
                    elif compression == "zstd":
                        filepath = filepath.with_suffix(".zst")

                if compression == "?":
                    self.assertRaises(
                        ValueError,
                        write_yaml,
                        filename=filepath,
                        data=data_expected,
                        mode="w",
                        compression="?",
                        level=None,
                    )
                    continue

                # Write to new file
                mode = "w"
                if compression in ("tar.bz2", "tar.gz", "tar.xz"):
                    mode += ":" + compression.split(".")[1]
                elif compression == "tgz":
                    mode += ":gz"

                if infer & ((compression is None) | add_file_extension):
                    write_yaml(
                        filename=filepath,
                        data=data_expected,
                        mode=mode,
                        compression="infer",
                        level=None,
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        write_yaml(
                            filename=filepath,
                            data=data_expected,
                            mode=mode,
                            compression="tar",
                            level=None,
                        )
                    else:
                        write_yaml(
                            filename=filepath,
                            data=data_expected,
                            mode=mode,
                            compression=compression,
                            level=None,
                        )

                # Read file contents
                if infer & ((compression is None) | add_file_extension):
                    data_observed = read_yaml(
                        filename=filepath,
                        mode="r",
                        compression="infer",
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if "." in compression:
                            mode = "r:" + compression.split(".")[1]
                        else:
                            mode = "r:gz"

                        data_observed = read_yaml(
                            filename=filepath,
                            mode=mode,
                            compression="tar",
                        )
                    else:
                        data_observed = read_yaml(
                            filename=filepath,
                            mode="r",
                            compression=compression,
                        )

                self.assertEqual(
                    data_expected,
                    data_observed,
                    "read_yaml() failed.\n"
                    "Parameters:\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{data_expected}'\n"
                    f"Observed: '{data_observed}'",
                )
