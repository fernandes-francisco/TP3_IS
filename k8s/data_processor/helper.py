import io
import csv

def dicts_to_csv(rows, header):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=header)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()
