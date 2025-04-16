import csv

# Function to write data to a CSV file
def write_to_csv(data: list[tuple], columns: list[str], file_name: str = "output.csv") -> int:
    try:
        with open(file_name, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)        # Write header
            writer.writerows(data)          # Write data rows
        return len(data)                    # Return number of rows written
    except Exception as e:
        raise Exception(f"Error writing to CSV: {e}")

# Function to fetch data from a ClickHouse table
def fetch_data(client, table_name: str, columns: list[str]):
    try:
        col_string = ", ".join(columns)
        query = f"SELECT {col_string} FROM {table_name}"
        return client.execute(query)
    except Exception as e:
        raise Exception(f"Error fetching data from ClickHouse: {e}")

