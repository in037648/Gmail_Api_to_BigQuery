def sanitize_subject(subject):
    keepcharacters = (' ', '.', '_')
    return "".join(c for c in subject if c.isalnum() or c in keepcharacters).rstrip().replace(" ", "_")

def format_column_names(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    df.columns = df.columns.str.replace(r'\W', '_', regex=True)
    return df

def clean_dataframe(df, original_columns):
    # Drop duplicate rows
    df = df.drop_duplicates()
    
    # Remove rows that contain column names
    column_name_row_mask = df.apply(lambda row: row.astype(str).str.contains('|'.join(original_columns)).all(), axis=1)
    df = df[~column_name_row_mask]
    
    return df
