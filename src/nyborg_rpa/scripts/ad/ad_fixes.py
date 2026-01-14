import datetime as dt
from pathlib import Path

import nbformat
from nbconvert import HTMLExporter
from nbconvert.preprocessors import ExecutePreprocessor

from nyborg_rpa.utils.pad import dispatch_pad_script


def ad_fixes(*, project_dir: str | Path = None):

    # config
    project_dir = project_dir or Path(r"C:\nyborg-rpa")
    output_dir = Path(r"J:\Drift\57. OS2sofd AD fix")
    notebooks = [
        project_dir / "src/nyborg_rpa/scripts/ad/ad_mail_attr_fix.ipynb",
        project_dir / "src/nyborg_rpa/scripts/ad/ad_new_sofd_users.ipynb",
    ]

    # set up notebook executor and HTML exporter
    ep = ExecutePreprocessor(timeout=600, allow_errors=False)
    html_exporter = HTMLExporter()

    # code to inject at the start of each notebook to remove pandas limits
    pandas_config = (
        "import pandas as pd\n"
        "pd.set_option('display.max_rows', None)\n"
        "pd.set_option('display.max_columns', None)\n"
        "pd.set_option('display.max_colwidth', None)\n"
        "pd.set_option('display.width', None)\n"
    )

    for nb_path in notebooks:

        nb_path = Path(nb_path).resolve()
        print(f"Running notebook {nb_path.as_posix()!r}...")

        # read notebook with UTF-8 encoding
        nb = nbformat.read(nb_path, as_version=nbformat.NO_CONVERT)

        # inject pandas config at the beginning
        config_cell = nbformat.v4.new_code_cell(pandas_config)
        nb.cells.insert(0, config_cell)

        # remove output limits
        for cell in nb.cells:
            if cell.cell_type == "code":
                cell.metadata["scrolled"] = False

        # prepare output path and create directory
        output_path = output_dir / f"{dt.datetime.now():%Y%m%d-%H%M%S}-{nb_path.stem}.html"
        output_dir.mkdir(parents=True, exist_ok=True)

        # execute notebook
        ep.preprocess(nb, {"metadata": {"path": str(nb_path.parent)}})

        # export to HTML and save
        (body, resources) = html_exporter.from_notebook_node(nb)
        output_path.write_text(body, encoding="utf-8")

        print(f"Saved to {output_path.as_posix()}\n")

    print(f"Finished processing {len(notebooks)} notebook(s).")


if __name__ == "__main__":
    dispatch_pad_script(fn=ad_fixes)
