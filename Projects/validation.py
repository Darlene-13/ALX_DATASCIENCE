# validation.py
def validate_field_data(df):
    errors = []
    if 'field_id' not in df.columns:
        errors.append("Missing field_id column.")
    if df['field_id'].isnull().any():
        errors.append("Null field IDs found.")
    if 'date' in df.columns and not df['date'].is_monotonic_increasing:
        errors.append("Dates not in order.")
    return errors

def validate_weather_data(df):
    errors = []
    if 'temperature' not in df.columns:
        errors.append("Missing temperature column.")
    if df['temperature'].isnull().mean() > 0.1:
        errors.append("Too many missing temperature values.")
    return errors
