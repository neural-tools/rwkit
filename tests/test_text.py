"""
Test text file I/O
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

import zstandard

from rwkit.rw_text import read_lines, read_text, write_lines, write_text


class TestText(unittest.TestCase):
    """TestText"""

    def test_read_text(self):
        """test_read_text"""

        text_list = [
            "This is a text",
            "This is\nmore text",
            "These\nare words\nof\na\nsentence",
        ]

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

        for (
            text,
            add_file_extension,
            compression,
            infer,
        ) in itertools.product(
            text_list,
            add_file_extension_list,
            compression_list,
            infer_list,
        ):
            text_expected = text

            with TemporaryDirectory() as tmpdir:
                filepath = Path(tmpdir) / "file"

                content = text
                content_binary = content.encode()

                # Write to file
                if compression is None:
                    with open(filepath, mode="w") as handle:
                        handle.write(content)
                elif compression == "bz2":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".bz2")
                    with bz2.open(filepath, mode="wb") as handle:
                        handle.write(content_binary)
                elif compression == "gzip":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".gz")
                    with gzip.open(filepath, mode="wb") as handle:
                        handle.write(content_binary)
                elif compression == "xz":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".xz")
                    with lzma.open(
                        filepath, format=lzma.FORMAT_XZ, mode="wb"
                    ) as handle:
                        handle.write(content_binary)
                elif compression == "zip":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".zip")
                    with zipfile.ZipFile(filepath, mode="w") as container_handle:
                        with container_handle.open("data", mode="w") as file_handle:
                            file_handle.write(content_binary)
                elif compression.startswith("tar") | (compression == "tgz"):
                    tar_mode = "w"
                    if add_file_extension:
                        filepath = filepath.with_suffix("." + compression)

                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if "." in compression:
                            tar_mode += ":" + compression.split(".")[1]
                        else:
                            tar_mode += ":gz"

                    with tarfile.open(filepath, mode=tar_mode) as container_handle:
                        tar_info = tarfile.TarInfo(name="data")
                        tar_info.size = len(content_binary)
                        container_handle.addfile(
                            tar_info, fileobj=BytesIO(content_binary)
                        )
                elif compression == "zstd":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".zst")
                    with zstandard.open(filepath, mode="w") as handle:
                        handle.write(content)
                else:
                    raise NotImplementedError(compression)

                # Read file contents
                if infer & ((compression is None) | add_file_extension):
                    text_observed = read_text(
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

                        text_observed = read_text(
                            filename=filepath,
                            mode=mode,
                            compression="tar",
                        )
                    else:
                        text_observed = read_text(
                            filename=filepath,
                            mode="r",
                            compression=compression,
                        )

                self.assertEqual(
                    text_expected,
                    text_observed,
                    "read_text() failed.\n"
                    "Parameters:\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{text_expected}'\n"
                    f"Observed: '{text_observed}'",
                )

    def test_write_text(self):
        """test_write_text"""

        with TemporaryDirectory() as tmpdir:
            filename = Path(tmpdir) / "file"

            # Unknown compression raises ValueError
            self.assertRaises(
                ValueError,
                write_text,
                filename=filename,
                text="",
                mode="w",
                compression="?",
            )

            # Integer raises TypeError
            self.assertRaises(
                TypeError,
                write_text,
                filename=filename,
                text=123,
                mode="w",
                compression="infer",
            )

            # List raises TypeError
            self.assertRaises(
                TypeError,
                write_text,
                filename=filename,
                text=[""],
                mode="w",
                compression="infer",
            )

        text_list = ["This is a text", "This is\nanother\ntext"]
        appendix_list = [" and here are more words", " and even\nmore\nwords"]

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

        for (
            text,
            add_file_extension,
            compression,
            infer,
        ) in itertools.product(
            text_list,
            add_file_extension_list,
            compression_list,
            infer_list,
        ):
            text_expected = text

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
                        write_text,
                        filename=filepath,
                        text=text_expected,
                        mode="w",
                        compression="?",
                    )
                    continue

                # Write to new file
                mode = "w"
                if compression in ("tar.bz2", "tar.gz", "tar.xz"):
                    mode += ":" + compression.split(".")[1]
                elif compression == "tgz":
                    mode += ":gz"

                if infer & ((compression is None) | add_file_extension):
                    write_text(
                        filename=filepath,
                        text=text,
                        mode=mode,
                        compression="infer",
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        write_text(
                            filename=filepath,
                            text=text,
                            mode=mode,
                            compression="tar",
                        )
                    else:
                        write_text(
                            filename=filepath,
                            text=text,
                            mode=mode,
                            compression=compression,
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

                text_observed = content

                self.assertEqual(
                    text_expected,
                    text_observed,
                    "write_text() failed.\n"
                    "Parameters:\n"
                    f"  mode: 'w'\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{text_expected}'\n"
                    f"Observed: '{text_observed}'",
                )

                # Compressed containers do not allow appending
                if compression in ("zip", "tar", "tar.bz2", "tar.gz", "tgz", "tar.xz"):
                    self.assertRaises(
                        ValueError,
                        write_text,
                        filename=filepath,
                        text="more words",
                        mode="a",
                        compression=compression,
                    )
                    continue

                text_appended_expected = text_expected
                for appendix in appendix_list:
                    text_appended_expected += appendix

                    # Append to existing file
                    mode = "a"
                    if infer & ((compression is None) | add_file_extension):
                        write_text(
                            filename=filepath,
                            text=appendix,
                            mode=mode,
                            compression="infer",
                        )
                    else:
                        write_text(
                            filename=filepath,
                            text=appendix,
                            mode=mode,
                            compression=compression,
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
                    elif compression == "tar":
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
                        with lzma.open(
                            filepath, format=lzma.FORMAT_XZ, mode="r"
                        ) as handle:
                            content = handle.read().decode()
                    elif compression == "zstd":
                        with zstandard.open(filepath, mode="r") as handle:
                            content = handle.read()
                    else:
                        raise NotImplementedError(compression)

                    text_appended_observed = content

                    self.assertEqual(
                        text_appended_expected,
                        text_appended_observed,
                        "write_text() failed.\n"
                        "Parameters:\n"
                        f"  mode: 'a'\n"
                        f"  compression: {compression}\n"
                        f"Expected: '{text_appended_expected}'\n"
                        f"Observed: '{text_appended_observed}'",
                    )

    def test_read_write_text(self):
        """test_read_write_text"""

        text_list = ["This is a text", "This is\nanother\ntext"]
        appendix_list = [" and here are more words", " and even\nmore\nwords"]

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

        for (
            text,
            add_file_extension,
            compression,
            infer,
        ) in itertools.product(
            text_list,
            add_file_extension_list,
            compression_list,
            infer_list,
        ):
            text_expected = text

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
                        write_text,
                        filename=filepath,
                        text=text,
                        mode="w",
                        compression="?",
                    )
                    continue

                # Write to new file
                mode = "w"
                if compression in ("tar.bz2", "tar.gz", "tar.xz"):
                    mode += ":" + compression.split(".")[1]
                elif compression == "tgz":
                    mode += ":gz"

                if infer & ((compression is None) | add_file_extension):
                    write_text(
                        filename=filepath,
                        text=text,
                        mode=mode,
                        compression="infer",
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        write_text(
                            filename=filepath,
                            text=text,
                            mode=mode,
                            compression="tar",
                        )
                    else:
                        write_text(
                            filename=filepath,
                            text=text,
                            mode=mode,
                            compression=compression,
                        )

                # Read file contents
                if infer & ((compression is None) | add_file_extension):
                    text_observed = read_text(
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

                        text_observed = read_text(
                            filename=filepath,
                            mode=mode,
                            compression="tar",
                        )
                    else:
                        text_observed = read_text(
                            filename=filepath,
                            mode="r",
                            compression=compression,
                        )

                self.assertEqual(
                    text_expected,
                    text_observed,
                    "read_write_text() failed.\n"
                    "Parameters:\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{text_expected}'\n"
                    f"Observed: '{text_observed}'",
                )

                # Append to file
                # Container formats do not allow appending
                if compression in ("zip", "tar", "tar.bz2", "tar.gz", "tgz", "tar.xz"):
                    self.assertRaises(
                        ValueError,
                        write_text,
                        filename=filepath,
                        text="more words",
                        mode="a",
                        compression=compression,
                    )
                    continue

                text_appended_expected = text_expected
                for appendix in appendix_list:
                    text_appended_expected += appendix

                    # Append to existing file
                    if infer & ((compression is None) | add_file_extension):
                        write_text(
                            filename=filepath,
                            text=appendix,
                            mode="a",
                            compression="infer",
                        )
                    else:
                        write_text(
                            filename=filepath,
                            text=appendix,
                            mode="a",
                            compression=compression,
                        )

                    # Read file contents
                    if infer & ((compression is None) | add_file_extension):
                        text_appended_observed = read_text(
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

                            text_appended_observed = read_text(
                                filename=filepath,
                                mode="a",
                                compression="tar",
                            )
                        else:
                            text_appended_observed = read_text(
                                filename=filepath,
                                mode="r",
                                compression=compression,
                            )

                    self.assertEqual(
                        text_appended_expected,
                        text_appended_observed,
                        "read_write_text() failed.\n"
                        "Parameters:\n"
                        f"  mode: 'a'\n"
                        f"  compression: {compression}\n"
                        f"Expected: '{text_appended_expected}'\n"
                        f"Observed: '{text_appended_observed}'",
                    )

    def test_read_lines(self):
        """test_read_lines"""

        lines_expected = ["These", "are words", "of a sentence"]
        content = "\n".join(lines_expected) + "\n"
        content_binary = content.encode()

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
                        handle.write(content)
                elif compression == "bz2":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".bz2")
                    with bz2.open(filepath, mode="wb") as handle:
                        handle.write(content_binary)
                elif compression == "gzip":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".gz")
                    with gzip.open(filepath, mode="wb") as handle:
                        handle.write(content_binary)
                elif compression == "xz":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".xz")
                    with lzma.open(
                        filepath, format=lzma.FORMAT_XZ, mode="wb"
                    ) as handle:
                        handle.write(content_binary)
                elif compression == "zip":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".zip")
                    with zipfile.ZipFile(filepath, mode="w") as container_handle:
                        with container_handle.open("data", mode="w") as file_handle:
                            file_handle.write(content_binary)
                elif compression.startswith("tar") | (compression == "tgz"):
                    tar_mode = "w"
                    if add_file_extension:
                        filepath = filepath.with_suffix("." + compression)

                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if "." in compression:
                            tar_mode += ":" + compression.split(".")[1]
                        else:
                            tar_mode += ":gz"

                    with tarfile.open(filepath, mode=tar_mode) as container_handle:
                        tar_info = tarfile.TarInfo(name="data")
                        tar_info.size = len(content_binary)
                        container_handle.addfile(
                            tar_info, fileobj=BytesIO(content_binary)
                        )
                elif compression == "zstd":
                    if add_file_extension:
                        filepath = filepath.with_suffix(".zst")
                    with zstandard.open(filepath, mode="w") as handle:
                        handle.write(content)
                else:
                    raise NotImplementedError(compression)

                # Read file contents
                if infer & ((compression is None) | add_file_extension):
                    lines_observed = read_lines(
                        filename=filepath, mode="r", compression="infer"
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if "." in compression:
                            mode = "r:" + compression.split(".")[1]
                        else:
                            mode = "r:gz"

                        lines_observed = read_lines(
                            filename=filepath, mode=mode, compression="tar"
                        )
                    else:
                        lines_observed = read_lines(
                            filename=filepath, mode="r", compression=compression
                        )

                self.assertEqual(
                    lines_expected,
                    lines_observed,
                    "read_lines() failed.\n"
                    "Parameters:\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{lines_expected}'\n"
                    f"Observed: '{lines_observed}'",
                )

                # Read file contents with chunksize
                # chunksize = 0 should raise ValueError
                with self.assertRaises(ValueError):
                    next(
                        read_lines(
                            filename=filepath,
                            mode="r",
                            compression="infer",
                            chunksize=0,
                        )
                    )

                # All chunksizes must return the same result
                for chunksize in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 1000):
                    if infer & ((compression is None) | add_file_extension):
                        lines_observed = []
                        for chunk in read_lines(
                            filename=filepath,
                            mode="r",
                            compression="infer",
                            chunksize=chunksize,
                        ):
                            lines_observed.extend(chunk)
                    else:
                        if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                            if "." in compression:
                                mode = "r:" + compression.split(".")[1]
                            else:
                                mode = "r:gz"

                            lines_observed = []
                            for chunk in read_lines(
                                filename=filepath,
                                mode=mode,
                                compression="tar",
                                chunksize=chunksize,
                            ):
                                lines_observed.extend(chunk)
                        else:
                            lines_observed = []
                            for chunk in read_lines(
                                filename=filepath,
                                mode="r",
                                compression=compression,
                                chunksize=chunksize,
                            ):
                                lines_observed.extend(chunk)

                    self.assertEqual(
                        lines_expected,
                        lines_observed,
                        "read_lines() failed.\n"
                        "Parameters:\n"
                        f"  compression: {compression}\n"
                        f"  chunksize:   {chunksize}\n"
                        f"Expected: '{lines_expected}'\n"
                        f"Observed: '{lines_observed}'",
                    )

    def test_write_lines(self):
        """test_write_lines"""

        # Unknown compression raises NotImplementedError
        with TemporaryDirectory() as tmpdir:
            filename = Path(tmpdir) / "file"
            self.assertRaises(
                ValueError,
                write_lines,
                filename=filename,
                lines=[""],
                mode="w",
                compression="?",
            )

        lines_expected = ["These", "Are", "Strings"]
        appendix = ["?", "one more line"]
        lines_appended_expected = lines_expected + appendix

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
                        write_lines,
                        filename=filepath,
                        lines=lines_expected,
                        mode="w",
                        compression="?",
                    )
                    continue

                # Write to new file
                mode = "w"
                if compression in ("tar.bz2", "tar.gz", "tar.xz"):
                    mode += ":" + compression.split(".")[1]
                elif compression == "tgz":
                    mode += ":gz"

                if infer & ((compression is None) | add_file_extension):
                    write_lines(
                        filename=filepath,
                        lines=lines_expected,
                        mode=mode,
                        compression="infer",
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        write_lines(
                            filename=filepath,
                            lines=lines_expected,
                            mode=mode,
                            compression="tar",
                        )
                    else:
                        write_lines(
                            filename=filepath,
                            lines=lines_expected,
                            mode=mode,
                            compression=compression,
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

                lines_observed = content.rstrip("\n").split("\n")

                self.assertEqual(
                    lines_expected,
                    lines_observed,
                    "write_lines() failed.\n"
                    "Parameters:\n"
                    f"  mode: 'w'\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{lines_expected}'\n"
                    f"Observed: '{lines_observed}'",
                )

                # Compressed containers do not allow
                if compression in ("zip", "tar", "tar.bz2", "tar.gz", "tgz", "tar.xz"):
                    self.assertRaises(
                        ValueError,
                        write_lines,
                        filename=filepath,
                        lines=appendix,
                        mode="a",
                        compression=compression,
                    )
                    continue

                # Append to existing file
                mode = "a"
                if infer & ((compression is None) | add_file_extension):
                    write_lines(
                        filename=filepath,
                        lines=appendix,
                        mode=mode,
                        compression="infer",
                    )
                else:
                    write_lines(
                        filename=filepath,
                        lines=appendix,
                        mode=mode,
                        compression=compression,
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
                elif compression == "tar":
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
                elif compression == "zstd":
                    with zstandard.open(filepath, mode="r") as handle:
                        content = handle.read()
                else:
                    raise NotImplementedError(compression)

                lines_appended_observed = content.rstrip("\n").split("\n")

                self.assertEqual(
                    lines_appended_expected,
                    lines_appended_observed,
                    "write_lines() failed.\n"
                    "Parameters:\n"
                    f"  mode: 'a'\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{lines_appended_expected}'\n"
                    f"Observed: '{lines_appended_observed}'",
                )

    def test_read_write_lines(self):
        """test_read_write_lines"""

        lines_expected = ["These", "are words", "of a sentence"]
        appendix = ["here are more", "words"]
        lines_appended_expected = lines_expected + appendix

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
                        write_lines,
                        filename=filepath,
                        lines=lines_expected,
                        mode="w",
                        compression="?",
                    )
                    continue

                # Write to new file
                mode = "w"
                if compression in ("tar.bz2", "tar.gz", "tar.xz"):
                    mode += ":" + compression.split(".")[1]
                elif compression == "tgz":
                    mode += ":gz"

                if infer & ((compression is None) | add_file_extension):
                    write_lines(
                        filename=filepath,
                        lines=lines_expected,
                        mode=mode,
                        compression="infer",
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        write_lines(
                            filename=filepath,
                            lines=lines_expected,
                            mode=mode,
                            compression="tar",
                        )
                    else:
                        write_lines(
                            filename=filepath,
                            lines=lines_expected,
                            mode=mode,
                            compression=compression,
                        )

                # Read file contents
                if infer & ((compression is None) | add_file_extension):
                    lines_observed = read_lines(
                        filename=filepath, mode="r", compression="infer"
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if "." in compression:
                            mode = "r:" + compression.split(".")[1]
                        else:
                            mode = "r:gz"

                        lines_observed = read_lines(
                            filename=filepath, mode=mode, compression="tar"
                        )
                    else:
                        lines_observed = read_lines(
                            filename=filepath, mode="r", compression=compression
                        )

                self.assertEqual(
                    lines_expected,
                    lines_observed,
                    "read_write_lines() failed.\n"
                    "Parameters:\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{lines_expected}'\n"
                    f"Observed: '{lines_observed}'",
                )

                # Compressed containers do not allow appending
                if compression in ("zip", "tar", "tar.bz2", "tar.gz", "tgz", "tar.xz"):
                    self.assertRaises(
                        ValueError,
                        write_lines,
                        filename=filepath,
                        lines=appendix,
                        mode="a",
                        compression=compression,
                    )
                    continue

                # Append to existing file
                mode = "a"
                if infer & ((compression is None) | add_file_extension):
                    write_lines(
                        filename=filepath,
                        lines=appendix,
                        mode=mode,
                        compression="infer",
                    )
                else:
                    write_lines(
                        filename=filepath,
                        lines=appendix,
                        mode=mode,
                        compression=compression,
                    )

                # Read file contents
                if infer & ((compression is None) | add_file_extension):
                    lines_appended_observed = read_lines(
                        filename=filepath, mode="r", compression="infer"
                    )
                else:
                    if compression in ("tar.bz2", "tar.gz", "tgz", "tar.xz"):
                        if "." in compression:
                            mode = "r:" + compression.split(".")[1]
                        else:
                            mode = "r:gz"

                        lines_appended_observed = read_lines(
                            filename=filepath, mode=mode, compression="tar"
                        )
                    else:
                        lines_appended_observed = read_lines(
                            filename=filepath, mode="r", compression=compression
                        )

                self.assertEqual(
                    lines_appended_expected,
                    lines_appended_observed,
                    "read_write_lines() failed.\n"
                    "Parameters:\n"
                    f"  mode: 'a'\n"
                    f"  compression: {compression}\n"
                    f"Expected: '{lines_appended_expected}'\n"
                    f"Observed: '{lines_appended_observed}'",
                )
