import pandas as pd

def read_csv_columns(file_path: str, delimiter: str = ","):
    df = pd.read_csv(file_path, delimiter=delimiter, nrows=1)
    return list(df.columns)

