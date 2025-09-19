from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from xlsxwriter.utility import xl_range

if TYPE_CHECKING:
    from xlsxwriter.worksheet import Worksheet


def df_to_excel_table(
    *,
    df: pd.DataFrame,
    filepath: Path | str,
    sheet_name: str = "Sheet1",
) -> None:
    """Write a pandas DataFrame to an Excel file as a formatted table with autofit columns."""

    filepath = Path(filepath)
    writer = pd.ExcelWriter(filepath, engine="xlsxwriter")
    df.to_excel(writer, sheet_name=sheet_name, startrow=1, header=False, index=False)

    ws: Worksheet = writer.sheets[sheet_name]
    (rows, cols) = df.shape
    column_settings = [{"header": column} for column in df.columns]

    # silence "Number stored as text" over the data range
    ws.ignore_errors({"number_stored_as_text": xl_range(1, 0, rows, cols - 1)})

    ws.add_table(0, 0, rows, cols - 1, {"columns": column_settings, "style": "Table Style Medium 2"})
    ws.set_column(0, cols - 1, 1)
    ws.autofit()

    writer.close()
