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
    wrap_cols: list[str] | None = None,
) -> None:
    """Write a pandas DataFrame to an Excel file as a formatted table with autofit columns.

    Args:
        df: DataFrame to write to Excel
        filepath: Path to save the Excel file
        sheet_name: Name of the worksheet
        wrap_cols: List of column names to enable text wrapping for
    """

    filepath = Path(filepath)
    writer = pd.ExcelWriter(filepath, engine="xlsxwriter")
    df.to_excel(writer, sheet_name=sheet_name, startrow=1, header=False, index=False)

    ws: Worksheet = writer.sheets[sheet_name]
    workbook = writer.book
    (rows, cols) = df.shape
    column_settings = [{"header": column} for column in df.columns]

    # silence "Number stored as text" over the data range
    ws.ignore_errors({"number_stored_as_text": xl_range(1, 0, rows, cols - 1)})

    ws.add_table(0, 0, rows, cols - 1, {"columns": column_settings, "style": "Table Style Medium 2"})

    # Create formats with top-left alignment
    wrap_format = workbook.add_format({"text_wrap": True, "valign": "top", "align": "left"})
    normal_format = workbook.add_format({"valign": "top", "align": "left"})

    # Apply formatting to columns
    if wrap_cols:
        wrap_col_indices = {df.columns.get_loc(col) for col in wrap_cols if col in df.columns}

        for col_idx in range(cols):
            if col_idx in wrap_col_indices:
                ws.set_column(col_idx, col_idx, None, wrap_format)
            else:
                ws.set_column(col_idx, col_idx, 1, normal_format)
    else:
        ws.set_column(0, cols - 1, 1, normal_format)

    ws.autofit()

    writer.close()
