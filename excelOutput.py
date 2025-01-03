from openpyxl import Workbook


def excelWriting(df_rep):
    # Extract the data from PySpark DataFrame
    data = df_rep.collect()  # Collect all rows into a list of Row objects

    # Create a new Excel workbook and sheet
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    
    # Get column names
    columns = df_rep.columns

    # Write column names to the first row in the Excel file
    ws.append(columns)

    # Write the data rows
    for row in data:
        ws.append(list(row))

    # Save the workbook to an Excel file
    output_path = "C:\\Users\\KIIT\\Desktop\\Nov@2024\\prcte\\pyspark101\\output\\output_file.xlsx"
    wb.save(output_path)

    print(f"DataFrame successfully exported to {output_path}")
