import subprocess
import os
import json
import polars as pl
import pandas as pd
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from pathlib import Path  # For robust path handling

def display_string_in_notepad(text):
    """
    Opens a Notepad window and displays the given string.

    Args:
        text: The string to display in Notepad.
    """

    try:
        # Create a temporary file to store the string
        temp_file = "temp.txt"  # You can change the filename if you want

        with open(temp_file, "w") as f:
            f.write(text)

        # Open Notepad with the temporary file
        subprocess.Popen(["nano", temp_file])  # Use Popen to run Notepad in the background

    except Exception as e:
        print(f"Error: {e}")


def dict_to_string(obj):
    res = json.dumps(obj, indent=4)
    return res



def round_float_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Rounds all float columns in a Polars DataFrame to 2 decimal places.

    Args:
        df: The input Polars DataFrame.

    Returns:
        A new Polars DataFrame with float columns rounded to 2 decimal places.
    """

    for col_name in df.columns:
        if df[col_name].dtype == pl.Float32 or df[col_name].dtype == pl.Float64:
            # print("detected float column: ", col_name)
            df = df.with_columns(
                pl.col(col_name).round(2)  # Use round directly on the column
            )

    return df


def export_polars_to_excel(df: pl.DataFrame, filepath: str) -> None:
    """
    Exports a Polars DataFrame to an Excel file (.xlsx) and adjusts column widths
    to fit the content without wrapping.

    Args:
        df: The Polars DataFrame to export.
        filepath: The path to the Excel file to create (e.g., "output.xlsx").
    """

    # Ensure filepath is a string
    if not isinstance(filepath, str):
        raise TypeError("filepath must be a string")

    # Convert filepath to a Path object for better handling
    filepath = Path(filepath)

    # Check if the parent directory exists.  If not, create it.
    if not filepath.parent.exists():
        filepath.parent.mkdir(parents=True, exist_ok=True) # Create directory tree

    try:
        # Convert Polars DataFrame to Pandas DataFrame
        pandas_df = df.to_pandas()

        # Create an Excel workbook and worksheet
        workbook = openpyxl.Workbook()
        worksheet = workbook.active

        # Write the Pandas DataFrame to the worksheet
        for row in dataframe_to_rows(pandas_df, index=False, header=True):
            worksheet.append(row)

        # Adjust column widths to fit content
        for column_cells in worksheet.columns:
            max_length = 0
            column = column_cells[0].column_letter  # Get the column name
            for cell in column_cells:
                try:  # Necessary to avoid error on empty cells
                    if cell.value:  # Check if cell.value is not None
                        cell_length = len(str(cell.value))
                        if cell_length > max_length:
                            max_length = cell_length
                except Exception as e:
                    print(f"Error getting length of cell value: {e}")

            adjusted_width = (max_length + 2)  # Add some padding
            worksheet.column_dimensions[column].width = adjusted_width


        # Save the workbook to the Excel file
        workbook.save(filepath)

        print(f"DataFrame successfully exported to: {filepath}")

    except Exception as e:
        print(f"An error occurred during export: {e}")
        raise  # Re-raise the exception to signal failure to the caller


def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None  # Or pl.NULL if you prefer Polars' null value
    


def format_float_to_string(number: float) -> str:
    """
    Formats a float number to a string with comma as thousand separator and point as decimal separator,
    rounded to 2 decimal places.

    Args:
        number: The float number to format.

    Returns:
        A string representation of the number, formatted with comma and point.
    """

    rounded_number = round(number, 2)
    integer_part   = int(rounded_number)
    decimal_part   = int((rounded_number - integer_part) * 100)

    formatted_integer = "{:,}".format(integer_part).replace(",", ".")  # Use . for thousand separator

    return f"{formatted_integer},{decimal_part:02d}"


def remove_df_underscore(df):
    colnames        = list(df.columns)
    spaced_colnames = []
    for col in colnames:
        spaced_col = col.replace("_", " ")
        spaced_colnames.append(spaced_col)
    
    df.columns = spaced_colnames
    return df


def write_df_to_excel(df, output):
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Convert the DataFrame to an Excel file
        df.to_excel(writer, sheet_name='Sheet1')
        
        # Get the xlsxwriter workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
    
        # Create a cell format with text wrapping enabled
        header_format = workbook.add_format({
            'text_wrap': True,  # Enable text wrapping
            'valign'   : 'vcenter',    
            'align'    : 'center',    
            'bold'     : True      
        })
        
        # Apply the multiline format to the header row (row 0)
        for col_num, value in enumerate(df.columns.values):
            # +1 to account for the index column
            worksheet.write(0, col_num + 1, value, header_format)
    
        # Set row height for the header row to accommodate multiple lines
        worksheet.set_row(0, 40)  # Height in points
        
        # Set the column width based on the maximum length in each column
        colnames  = list(df.columns)
        widthlist = []
        for col in colnames:
            max_len = max(df[col].astype(str).map(len).max(), len(col))
            widthlist.append(max_len + 2)
        
        for idx, col in enumerate(df.columns):
            colwidth = widthlist[idx]
            worksheet.set_column(idx + 1, idx + 1, colwidth)
        
        # Also adjust the index column (column 0)
        max_index_len = max(len(str(i)) for i in df.index)
        index_width   = max(max_index_len, len(str(df.index.name) if df.index.name else '')) + 2
        worksheet.set_column(0, 0, index_width)
    return output