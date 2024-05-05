import pandas as pd

def nan_null_zero_datatypes(df):
    """
    Analyzes a DataFrame to identify the presence of NaNs, blanks, zeros, and different data types in each column.

    Parameters:
        df (DataFrame): The DataFrame to be analyzed.

    Returns:
        DataFrame: A DataFrame containing the analysis results with the following columns:
            - 'Column Name': The names of the columns in the DataFrame.
            - '# Non Zero Values': The count of non-null and non-zero values for each column.
            - '# NAs': The count of NaNs (missing values) for each column.
            - '# Blanks': The count of blank values (e.g., empty strings) for each column.
            - '# Zeros': The count of zeros for each column.
            - '# Data Types': The number of unique data types for each column.
            - 'Data Types': The unique data types present in each column.
    """
    have_values = [(df[col].notna() & (df[col] != 0)).sum() for col in df.columns]
    nans = [df[col].isna().sum() for col in df.columns]
    blanks = [(df[col] == ' ').sum() for col in df.columns]
    zeros = [(df[col] == 0).sum() for col in df.columns]
    data_types = [df[col].apply(lambda x: type(x)).unique() for col in df.columns]
    len_data_types = [len(df[col].apply(lambda x: type(x)).unique()) for col in df.columns]  

    df_clean_check = pd.DataFrame({
        'Column Name': df.columns,
        '# Non Zero Values': have_values,
        '# NAs': nans,
        '# Blanks': blanks,
        '# Zeros': zeros,
        '# Data Types': len_data_types,
        'Data Types': data_types
    })

    return df_clean_check    

