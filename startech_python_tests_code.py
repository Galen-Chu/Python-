import json
import pandas as pd
from datetime import datetime

# Load the JSON file
with open('output_clean_date_technical.json', 'r') as file:
    data = json.load(file)

# Munging data into a consistent json array
if 'historicalPriceFull' in data and 'historical' in data['historicalPriceFull']:
    data['historicalPriceFull'] = data['historicalPriceFull']['historical']
    for record in data['historicalPriceFull']:
        record['symbol'] = '1101.TW'

# Convert JSON data to DataFrame
dataframes = []
for key, records in data.items():
    dataframes.append(pd.DataFrame(records))
df = pd.concat(dataframes, ignore_index=True)

# Ensure the 'date' column is in datetime format
df['date'] = pd.to_datetime(df['date'], errors='coerce')

# Merge rows with the same 'date' and 'symbol'
def merge_rows(df):
    df = df.groupby(['symbol', 'date'], as_index=False).first()
    return df

df = merge_rows(df)

# Helper function to get the date range from a quarterly period
def get_date_range(year, period):
    periods = {
        "Q1": (f"{year}-01-01", f"{year}-03-31"),
        "Q2": (f"{year}-04-01", f"{year}-06-30"),
        "Q3": (f"{year}-07-01", f"{year}-09-30"),
        "Q4": (f"{year}-10-01", f"{year}-12-31"),
    }
    return pd.Timestamp(periods[period][0]), pd.Timestamp(periods[period][1])

# Process data with a 'period' value
# Convert date to range and position quarterly rows above corresponding daily rows
def process_period_data(df):
    all_rows = []
    for symbol, group in df.groupby('symbol'):
        quarterly_rows = []
        daily_rows = []
        for _, row in group.iterrows():
            if pd.notna(row['period']):
                year = row['calendarYear']
                period = row['period']
                start_date, end_date = get_date_range(year, period)
                row['date'] = f"{start_date.date()}~{end_date.date()}"  # Retain original format in date for quarterly rows
                formatted_start = start_date.strftime('%d-%b-%Y')
                formatted_end = end_date.strftime('%d-%b-%Y')
                row['label'] = f"{formatted_start}~{formatted_end}"  # Update label to match formatted date range
                quarterly_rows.append(row)
            else:
                row['date'] = row['date'].strftime('%Y-%m-%d')  # Ensure daily rows are in YYYY-MM-DD format
                daily_rows.append(row)
        # Insert quarterly rows above daily rows for each quarter
        processed_daily_indices = set()
        for quarterly_row in quarterly_rows:
            start_date, end_date = get_date_range(quarterly_row['calendarYear'], quarterly_row['period'])
            daily_within_quarter = [row for i, row in enumerate(daily_rows) if start_date <= pd.to_datetime(row['date']) <= end_date]
            all_rows.append(quarterly_row)
            all_rows.extend(daily_within_quarter)
            processed_daily_indices.update(i for i, row in enumerate(daily_rows) if start_date <= pd.to_datetime(row['date']) <= end_date)
        # Add remaining daily rows that do not fall within any quarterly period
        unprocessed_daily_rows = [row for i, row in enumerate(daily_rows) if i not in processed_daily_indices]
        all_rows.extend(unprocessed_daily_rows)
    return pd.DataFrame(all_rows)

df = process_period_data(df)

# Method to forward-fill NaN values in all columns
def forward_fill_data(df):
    return df.ffill()

df = forward_fill_data(df)

# Method to backfill NaN values in all columns
def back_fill_data(df):
    return df.ffill().bfill()

df = back_fill_data(df)

# Remove columns where all values are NaN
def remove_all_nan_columns(df):
    return df.dropna(axis=1, how='all')

df = remove_all_nan_columns(df)

# Save the processed DataFrame
df.to_csv('processed_data.csv', index=False)
#%%
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import pandas as pd

# Load the processed data
merged_df = pd.read_csv('processed_data.csv')

# Drop non-numerical columns or encode them if needed
merged_df = merged_df.select_dtypes(include=["number"])  # Keep only numerical columns

# Define target variable (e.g., 'netIncomeGrowth') and features
target = 'netIncomeGrowth'  # Replace with your target column
features = merged_df.drop(columns=[target])  # Drop target column to keep features

# Split into train and test sets
X_train, X_test, y_train, y_test = train_test_split(features, merged_df[target], test_size=0.2, random_state=42)

# Initialize and train the model
model = LinearRegression()
model.fit(X_train, y_train)

# Predict on the test set
y_pred = model.predict(X_test)

# Evaluate the model
mse = mean_squared_error(y_test, y_pred)
print(f"Mean Squared Error: {mse}")