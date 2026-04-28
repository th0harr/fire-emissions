'''
Get means and standard deviations of carbon content, and get ratio of fossil and biogenic content which sum to 1.

The input is the material composition spreadsheet (material_composition.xlsx) containing a sheet for eeach furniture class,
with columns 'carbon_content', 'p_biogenic', and 'p_fossil', that was assembled from LCA studies.

The output is an excel sheet 'carbon_content_means.xlsx' with means and standard deviations for
carbon_content, and ratio_biog, ratio_fossil which are mean proportions of fossil and biogenic content, scaled to 1.
ratio_biog and ratio_fossil are related in the following way: ratio_biog = 1 - ratio_fossil.
'''

# Import libraries
import pandas as pd

# Load all sheets of the excel file as a dictionary
mat_comp = pd.read_excel('local/material_composition.xlsx', sheet_name=None) # Replace with accurate filepath

# Create an empty list of rows
rows = []

# Loop over each sheet
for sheet_name, df in list(mat_comp.items())[3:24]:
    # Create a dictionary with sheet name
    row_data = {'sheet_name': sheet_name}

    # Get mean and sd for carbon_content
    row_data['mean_carbon_content'] = df['carbon_content'].mean()
    row_data['sd_carbon_content'] = df['carbon_content'].std()

    # Get mean p_fossil and p_biogenic
    mean_p_fossil = df['p_fossil'].mean()
    mean_p_biogenic = df['p_biogenic'].mean()


    # Get ratio_fossil and ratio_biog scaled to sum to 1
    row_data['ratio_fossil'] = mean_p_fossil/(mean_p_fossil+mean_p_biogenic)
    row_data['ratio_biogenic'] = mean_p_biogenic/(mean_p_fossil+mean_p_biogenic)    

    # Input into rows list
    rows.append(row_data)

# Create one dataframe
summary_df = pd.DataFrame(rows)

# Save in an excel sheet
summary_df.to_excel('local/carbon_content_means.xlsx')