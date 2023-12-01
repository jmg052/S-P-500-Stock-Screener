#%% Import required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import wrds
import yfinance as yf
import tkinter as tk
from tkinter import ttk, Text, Label, Entry, Button, messagebox
from tkinter import *
from datetime import datetime, timedelta
import os
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.dates as mdates

#%% Function to scrape S&P 500 tickers from Wikipedia
def fetch_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'wikitable sortable'})
        tickers = []
        for row in table.findAll('tr')[1:]:
            ticker = row.findAll('td')[0].text
            tickers.append(ticker.strip())
        return tickers
    else:
        print(f"Failed to fetch page, status code: {response.status_code}")
        return []

# Get S&P 500 tickers
sp500_tickers = fetch_sp500_tickers()
print(sp500_tickers[:5])

#%% Time for Data Retrieval
# Calculate yesterday's date
yesterday = datetime.now() - timedelta(1)
yesterday_date = yesterday.strftime('%Y-%m-%d')

# Calculate the date 5 years ago from yesterday
five_years_ago = yesterday - timedelta(days=365 * 5)
five_years_ago_date = five_years_ago.strftime('%Y-%m-%d')

#%% Establish connection to WRDS
db = wrds.Connection(wrds_username='jmg052')

# Prepare the tickers string for SQL IN clause
tickers_string = ', '.join(f"'{ticker}'" for ticker in sp500_tickers)

# Define the SQL query for retrieving the financial ratios and stock price data for S&P 500 companies
sql_query_finratiofirm_with_price = f"""
WITH RatioData AS (
    SELECT 
        TICKER, 
        public_date AS "Financial Ratios Date",
        pe_inc AS "Price/Earnings Ratio",
        ptb AS "Price/Book Ratio",
        divyield AS "Dividend Yield",
        roe AS "Return on Equity",
        npm AS "Net Profit Margin",
        de_ratio AS "Debt/Equity Ratio",
        PEG_trailing AS "PEG Ratio",
        curr_ratio AS "Current Ratio",
        at_turn AS "Asset Turnover"
    FROM 
        wrdsapps_finratio.firm_ratio
    WHERE 
        public_date = '2022-11-30'
        AND TICKER IN ({tickers_string})
), 
PriceData AS (
    SELECT 
        tic AS TICKER,
        datadate AS "Stock Price Date",
        prccd AS "Closing Price",
        ajexdi AS "Adjustment Factor"
    FROM 
        comp.secd
    WHERE 
        datadate BETWEEN '{five_years_ago_date}' AND '{yesterday_date}'
)
SELECT 
    r.*,
    p."Stock Price Date",
    p."Closing Price",
    p."Adjustment Factor"
FROM 
    RatioData r
JOIN 
    PriceData p ON r.TICKER = p.TICKER
ORDER BY 
    r.TICKER
"""

# Execute the query and fetch the data
BIGDATA = db.raw_sql(sql_query_finratiofirm_with_price)

# Display the first few rows of the combined data
print(BIGDATA.head())

db.close()


#%% Constants and Globals
csv_file = BIGDATA
columns_to_round = [
    "Price/Earnings Ratio",
    "Price/Book Ratio",
    "Dividend Yield",
    "Return on Equity",
    "Net Profit Margin",
    "Debt/Equity Ratio",
    "PEG Ratio",
    "Current Ratio",
    "Asset Turnover"
]
abbreviations = {
    "Price/Earnings Ratio": "P/E",
    "Price/Book Ratio": "P/B",
    "Dividend Yield": "Div Yld",
    "Return on Equity": "ROE",
    "Net Profit Margin": "NPM",
    "Debt/Equity Ratio": "D/E",
    "PEG Ratio": "PEG",
    "Current Ratio": "Curr Ratio",
    "Asset Turnover": "Asset TO",
    "Stock Price Date": "Date"
}
selected_filter_options = {
    "Price/Earnings Ratio": None,
    "Price/Book Ratio": None,
    "Dividend Yield": None,
    "Return on Equity": None,
    "Net Profit Margin": None,
    "Debt/Equity Ratio": None,
    "PEG Ratio": None,
    "Current Ratio": None,
    "Asset Turnover": None,
    "Stock Price": None
}

# Setup filter options and labels
filter_options = [
    ("Price/Earnings Ratio:", [
        "Any",
        "Low (<15)",
        "Profitable (>0)",
        "High (>50)"
    ]),
    ("Price/Book Ratio:", [
        "Any",
        "Low (<1)",
        "High (>5)"
    ]),
    ("Dividend Yield:", [
        "Any",
        "None (0%)",
        "Positive (>0%)",
        "High (>5%)",
        "Very High (>10%)"
    ]),
    ("Return on Equity:", [
        "Any",
        "Positive (>0%)",
        "Negative (<0%)",
        "Very Positive (>30%)",
        "Very Negative (<-15%)"
    ]),
    ("Net Profit Margin:", [
        "Any",
        "Positive (>0%)",
        "Negative (<0%)",
        "High (>20%)",
        "Very Negative (<-20%)"
    ]),
    ("Debt/Equity Ratio:", [
        "Any",
        "Low (<0.1)",
        "High (>0.5)"
    ]),
    ("PEG Ratio:", [
        "Any",
        "Low (<1)",
        "High (>2)"
    ]),
    ("Current Ratio:", [
        "Any",
        "Low (<1)",
        "High (>3)"
    ]),
    ("Asset Turnover:", [
        "Any",
        "Low (<0.5)",
        "High (>1)"
    ]),
    ("Stock Price:", [
        "Any",
        "Low (<$10)",
        "High (>$100)"
    ])
]
#%% Helper Functions
# Function to map filter options to numerical values
def map_filter_option_to_value(option, ratio_type):
    if option == "Any":
        return None
    elif ratio_type == "Price/Earnings Ratio":
        return {"Low (<15)": 15, "Profitable (>0)": 0, "High (>50)": 50}.get(option)
    elif ratio_type == "Price/Book Ratio":
        return {"Low (<1)": 1, "High (>5)": 5}.get(option)
    elif ratio_type == "Dividend Yield":
        return {"None (0%)": 0, "Positive (>0%)": 0.0001, "High (>5%)": 0.05, "Very High (>10%)": 0.1}.get(option)
    elif ratio_type == "Return on Equity":
        return {"Positive (>0%)": 0.0001, "Negative (<0%)": -0.0001, "Very Positive (>30%)": 0.30, "Very Negative (<-15%)": -0.15}.get(option)
    elif ratio_type == "Net Profit Margin":
        return {"Positive (>0%)": 0.0001, "Negative (<0%)": -0.0001, "High (>20%)": 0.20, "Very Negative (<-20%)": -0.20}.get(option)
    elif ratio_type == "Debt/Equity Ratio":
        return {"Low (<0.1)": 0.1, "High (>0.5)": 0.5}.get(option)
    elif ratio_type == "PEG Ratio":
        return {"Low (<1)": 1, "High (>2)": 2}.get(option)
    elif ratio_type == "Current Ratio":
        return {"Low (<1)": 1, "High (>3)": 3}.get(option)
    elif ratio_type == "Asset Turnover":
        return {"Low (<0.5)": 0.5, "High (>1)": 1}.get(option)
    elif ratio_type == "Stock Price":
        return {"Low (<$10)": 10, "High (>$100)": 100}.get(option)

# Function to apply abbreviations to the column names
def abbreviate_column_names(dataframe, abbreviations):
    dataframe = dataframe.rename(columns=abbreviations)
    return dataframe

# Function to retrieve stock data for a given ticker
def get_stock_data(ticker):
    # Ensure ticker is in the correct format (assuming your DataFrame uses uppercase tickers)
    ticker = ticker.upper()

    # Filter BIGDATA for the specified ticker
    stock_data = BIGDATA[BIGDATA['ticker'] == ticker]

    # Sort the data by date if it's not already sorted
    stock_data = stock_data.sort_values(by='Stock Price Date')

    return stock_data

# Function to apply filters
def apply_filters():
    print("Filter button clicked.")
    try:
        # Convert 'Stock Price Date' to datetime for comparison
        BIGDATA['Stock Price Date'] = pd.to_datetime(BIGDATA['Stock Price Date'])

        # Group by ticker and select the row with the most recent 'Stock Price Date' for each ticker
        latest_data = BIGDATA.groupby('ticker').apply(lambda x: x.nlargest(1, 'Stock Price Date')).reset_index(drop=True)

        # Initialize a combined filter condition with all True values
        combined_filter = pd.Series([True] * len(latest_data), index=latest_data.index)

        for label, combobox in filter_comboboxes.items():
            selected_option = combobox.get()
            category = label[:-1]  # Remove the trailing colon
            selected_filter_options[category] = selected_option

        # Initialize filter description string
        filter_descriptions = []

        for category, selected_option in selected_filter_options.items():
            if selected_option and selected_option != "Any":
                # Add the filter description to the list
                filter_descriptions.append(f"{abbreviations.get(category, category)}: {selected_option}")
                filter_value = map_filter_option_to_value(selected_option, category)

                if filter_value is not None:
                    # Determine if the filter should be 'greater than' or 'less than'
                    if "Low" in selected_option or "Negative" in selected_option or "None" in selected_option:
                        combined_filter &= (latest_data[category] < filter_value)
                    elif "High" in selected_option or "Positive" in selected_option or "Profitable" in selected_option:
                        combined_filter &= (latest_data[category] > filter_value)
        
          # Apply the combined filter to the latest data
        filtered_data = latest_data[combined_filter]

        # Abbreviate column names
        filtered_data = abbreviate_column_names(filtered_data, abbreviations)

        # Sort the filtered data by ticker
        filtered_data = filtered_data.sort_values(by='ticker')

        # Clear the existing content in the result_text widget
        result_text.delete(1.0, tk.END)

        # Insert the active filters as a message
        active_filters_message = "Active Filters: " + ", ".join(filter_descriptions) if filter_descriptions else "No active filters"
        filters_label.config(text=active_filters_message)

        # Insert the filtered data
        result_text.insert(tk.INSERT, filtered_data.to_string(index=False))

    except Exception as e:
        print(f"An error occurred: {str(e)}")
#%% Big Data preprocessing
# Calculate the adjusted close price
BIGDATA['Stock Price'] = BIGDATA['Closing Price'] / BIGDATA['Adjustment Factor']
# Drop the unnecessary columns
BIGDATA.drop(columns=['Closing Price', 'Adjustment Factor','Financial Ratios Date'], inplace=True)

# Apply rounding to each specified column
for column in columns_to_round:
    if column in BIGDATA.columns:
        BIGDATA[column] = BIGDATA[column].round(3)

    # Display the updated DataFrame
    print(BIGDATA.head())

#%% GUI Setup
window = tk.Tk()
window.title("Stock Screener")

# Set the initial size of the window (width x height)
window.geometry("1200x800")  # You can adjust the size as needed

# Create a notebook widget
notebook = ttk.Notebook(window)
notebook.pack(fill='both', expand=True)

# Create two tabs: One for filters and another for stock search and plot
filters_frame = ttk.Frame(notebook)
plot_frame = ttk.Frame(notebook)
notebook.add(filters_frame, text="Filters")
notebook.add(plot_frame, text="Stock Search & Plot")

filter_comboboxes = {}
for label, options in filter_options:
    filter_label = Label(filters_frame, text=label)
    filter_label.pack()
    filter_combobox = ttk.Combobox(filters_frame, values=options)
    filter_combobox.pack()
    filter_comboboxes[label] = filter_combobox

# Apply Filters button in filters_frame
apply_filters_button = Button(filters_frame, text="Apply Filters", command=apply_filters, height=2, width=15)
apply_filters_button.pack()

# Label to show active filters in filters_frame
filters_label = tk.Label(filters_frame, text="Active Filters: None", anchor='w')
filters_label.pack(fill='x')

# Result Text widget in filters_frame
result_text = Text(filters_frame, height=25, width=80)
result_text.pack(fill='both', expand=True)

# Ticker search label, entry, and button in plot_frame
ticker_label = Label(plot_frame, text="Enter Stock Ticker:")
ticker_label.pack()
ticker_entry = Entry(plot_frame)
ticker_entry.pack()
search_button = Button(plot_frame, text="Search Ticker", command=lambda: plot_stock_time_series(ticker_entry.get().upper()))
search_button.pack()


#%% Plotting the Stock Price Time Series
# Declare global variables for the figure, axis, and canvas
global fig, ax, canvas

def plot_stock_time_series(ticker):
    global fig, ax, canvas

    # Retrieve stock data
    stock_data = get_stock_data(ticker)
    if stock_data.empty:
        messagebox.showerror("Error", f"No data available for {ticker}")
        return

    # Calculate moving averages
    stock_data['50-day SMA'] = stock_data['Stock Price'].rolling(window=50).mean()
    stock_data['200-day SMA'] = stock_data['Stock Price'].rolling(window=200).mean()

    # Create a figure and axis for the plot, or clear the existing one
    if 'fig' in globals() and 'ax' in globals():
        ax.clear()
    else:
        fig, ax = plt.subplots()

    plt.style.use('seaborn-darkgrid')  # Use a stylish theme

    # Formatting the Date Axis: Label every 3 months in 'MM-YYYY' format
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%Y'))

    # Plotting the stock price data and SMAs
    ax.plot(stock_data['Stock Price Date'], stock_data['Stock Price'], label='Stock Price', color='teal', linewidth=2)
    ax.plot(stock_data['Stock Price Date'], stock_data['50-day SMA'], label='50-day SMA', color='orange', linewidth=2)
    ax.plot(stock_data['Stock Price Date'], stock_data['200-day SMA'], label='200-day SMA', color='purple', linewidth=2)

    # Adding labels, title, gridlines, and legend
    ax.set_title(f"{ticker} Stock Price with 50-day & 200-day SMA", fontsize=14, fontweight='bold')
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Price', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    ax.legend()
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    # Embedding the plot in the plot_frame or updating it
    if 'canvas' in globals():
        canvas.draw()
    else:
        canvas = FigureCanvasTkAgg(fig, master=plot_frame)
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(fill='both', expand=True)
# Run the GUI event loop
window.mainloop()
# %%
