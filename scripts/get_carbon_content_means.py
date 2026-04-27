'''
Get means and standard deviations of carbon content, proportion biogenic, and proportion fossil.

The input is the material composition spreadsheet (material_composition.xlsx) containing a sheet for eeach furniture class,
with columns 'carbon_content', 'p_biogenic', and 'p_fossil', that was assembled from LCA studies.

The output is an excel sheet 'carbon_content_means.xlsx' with means and standard deviations for
carbon_content, p_biogenic, and p_fossil.

'''

# Import libraries
import pandas as pd

# Load all sheets of the excel file as a dictionary
mat_comp = pd.read_excel('local/material_composition.xlsx', sheet_name=None) # Replace with accurate filepath

# Get relevant column names
cols = ['carbon_content', 'p_biogenic', 'p_fossil']

# Create an empty list of rows
rows = []

# Loop over each sheet
for sheet_name, df in list(mat_comp.items())[3:24]:
    # Create a dictionary with sheet name
    row_data = {'sheet_name': sheet_name}
    # Get mean and sd for each column in a subset
    for col in cols:
        row_data[f'mean_{col}'] = df[col].mean()
        row_data[f'sd_{col}'] = df[col].std()
    # Input into rows list
    rows.append(row_data)

# Create one dataframe
summary_df = pd.DataFrame(rows)

# Save in an excel sheet
summary_df.to_excel('local/carbon_content_means.xlsx')