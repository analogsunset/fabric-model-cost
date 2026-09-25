"""Convert the `# %%` notebooks in notebooks/ to .ipynb for Fabric's "Import notebook".

    python tools/py2ipynb.py            # writes build/*.ipynb
"""
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent


def cells(source):
    out, kind, buf = [], None, []

    def flush():
        lines = buf[:]
        while lines and not lines[-1].strip():
            lines.pop()
        if kind and lines:
            if kind == "markdown":
                lines = [l[2:] if l.startswith("# ") else l.lstrip("#") for l in lines]
            text = [l + "\n" for l in lines]
            text[-1] = text[-1].rstrip("\n")
            cell = {"cell_type": kind, "metadata": {}, "source": text}
            if kind == "code":
                cell.update(execution_count=None, outputs=[])
            out.append(cell)

    for line in source.splitlines():
        if line.startswith("# %%"):
            flush()
            kind, buf = ("markdown" if "[markdown]" in line else "code"), []
        else:
            buf.append(line)
    flush()
    return out


def main():
    build = ROOT / "build"
    build.mkdir(exist_ok=True)
    for py in sorted((ROOT / "notebooks").glob("*.py")):
        nb = {
            "nbformat": 4, "nbformat_minor": 5,
            "metadata": {"kernel_info": {"name": "synapse_pyspark"},
                         "language_info": {"name": "python"}},
            "cells": cells(py.read_text(encoding="utf-8")),
        }
        target = build / (py.stem + ".ipynb")
        target.write_text(json.dumps(nb, indent=1), encoding="utf-8")
        print(target.relative_to(ROOT))


if __name__ == "__main__":
    main()
