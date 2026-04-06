
## Inventory Database structure

```
inventory_db/
├─ raw/
│ ├─ showrooms/ 	# Manually curated showroom inventories (Excel)
│ ├─ surveys/ 		# Survey exports (JISC wide format)
│ └─ insurance/ 	# Insurance or property inventory data
│
├─ config/
│  └─ vocab/
│     └─ mapping_list.xlsx   # List of inventory mappings
│
├─ database/
│ ├─ pooled_inventory.sqlite 	# Main SQLite database
│ │ ├─ sources
│ │ │ ├─ source_id [PK]			# unique source identifier
│ │ │ ├─ data_source_type 		# survey / showroom / insurance
│ │ │ ├─ source_description		# brief description of dataset
│ │ │ ├─ source_org				# organisation providing data (if applicable)
│ │ │ ├─ file_name 				# original file name
│ │ │ ├─ file_path				# local file path
│ │ │ ├─ url					# source URL (if applicable)
│ │ │ ├─ date_collected 		# date data was originally collected
│ │ │ ├─ date_imported_utc 		# timestamp of DB import
│ │ │ └─ notes 					# additional metadata notes
│ │ │
│ │ ├─ inventory_observations   # item-level inventory observations
│ │ │ ├─ obs_id [PK]				# unique observation identifier
│ │ │ ├─ response_id				# JISC response identifier
│ │ │ ├─ source_id [FK]			# link to sources table
│ │ │ ├─ room_type [FK]			# room in which item is located
│ │ │ ├─ item_name [FK]			# internal item identifier
│ │ │ └─ count 					# number of items observed
│ │ │
│ │ ├─ dwelling_observations		# dwelling-level room count observations
│ │ │ ├─ dwelling_id [PK]		# dwelling observation identifier
│ │ │ ├─ response_id				# response identifier
│ │ │ ├─ source_id 				# link to sources table
│ │ │ ├─ room_type 				# room that is counted
│ │ │ └─ count 					# number of rooms observed
│ │ │
│ │ ├─ survey_comments			# extracted survey comments
│ │ │ ├─ comment_obs_id [PK]		# comment observation identifier
│ │ │ ├─ response_id				# response identifier
│ │ │ ├─ source_id 				# link to sources table
│ │ │ ├─ comment_type 			# controlled comment category
│ │ │ └─ comment_text				# comment string (free-text)
│ │ │
│ │ ├─ item_dictionary			# fixed item vocabulary
│ │ │ ├─ item_name [PK]			# internal item identifier
│ │ │ ├─ item_description 		# user-facing item label
│ │ │ ├─ item_mass 				# nominal mass (kg)
│ │ │ ├─ furniture_class [FK] 		# associated furniture class
│ │ │ └─ notes 					# item-level notes
│ │ │
│ │ ├─ furniture					# fixed furniture vocabulary
│ │ │ ├─ furniture_class [PK]	# furniture class identifier
│ │ │ ├─ furniture_description	# user-facing class description
│ │ │ ├─ class_contains			# examples of items in class
│ │ │ ├─ kgC_kg					# carbon mass per kg item (kgC/kg)
│ │ │ ├─ ratio_fossil			# fossil carbon fraction
│ │ │ ├─ ratio_biog				# biogenic carbon fraction
│ │ │ └─ notes					# class-level notes
│ │ │
│ │ ├─ room							# fixed room vocabulary
│ │ │ ├─ room_type [PK]			# internal room identifier
│ │ │ ├─ room_description		# user-facing room label
│ │ │ ├─ room_size					# average room size (m²)
│ │ │ ├─ size_assumed				# true / false
│ │ │ ├─ assumption_notes		# description of assumption
│ │ │ └─ notes					# room-level notes
│ │ │
│ │ ├─ assumed_inventory			# Assumed household items
│ │ │ ├─ assumed_item_id [PK]	# assumed item row identifier
│ │ │ ├─ room_type [FK]				# internal room identifier
│ │ │ ├─ item_name [FK]				# internal item identifier
│ │ │ ├─ count_assumed			# estimated item count
│ │ │ └─ assumption_notes 		# assumption text description
│ │ │
│ │ └─ ingest_log					# Ingest records for auditing
│ │ │ ├─ ingest_id [PK]			# unique ingest run identifier
│ │ │ ├─ source_id [FK]			# link to sources table
│ │ │ ├─ data_source_type		# type of data ingested
│ │ │ ├─ action					# ingest action performed
│ │ │ ├─ status					# success / failure status
│ │ │ ├─ message				# log message or error summary
│ │ │ ├─ started_utc			# ingest start timestamp
│ │ │ ├─ finished_utc			# ingest end timestamp
│ │ │ ├─ rows_inserted			# number of rows added
│ │ │ └─ rows_deleted			# number of rows removed
│ │ │
│ │ └─ item_count_pmf				# Item count probability mass function
│ │ │ ├─ item_pmf_id [PK]		# unique row identifier
│ │ │ ├─ item_name	 [FK]			# item identifier
│ │ │ ├─ room_type	 [FK]			# room identifier
│ │ │ ├─ count_value				# count value identifier
│ │ │ ├─ item_probability		# probability of count value
│ │ │ └─ item_pmf_notes			# notes
│ │ │
│ │ └─ item_count_summary			# Estimated item count
│ │ │ ├─ count_summary_id [PK]	# unique row identifier
│ │ │ ├─ item_name [FK]			# item identifier
│ │ │ ├─ room_type	 [FK]			# room identifier
│ │ │ ├─ expected_count_mean		# computed mean count
│ │ │ ├─ count_ci_lower			# lower confidence interval
│ │ │ ├─ count_ci_upper			# upper confidence interval
│ │ │ ├─ n_observations 			# total observations
│ │ │ └─ count_summary_notes		# notes
│ │ │
│ │ └─ room_carbon_stock					# Estimated carbon stock
│ │   ├─ carbon_summary_id [PK]			# unique row identifier
│ │   ├─ room_type [FK]					# room identifier
│ │   ├─ expected_total_carbon_kgC		# total carbon mass
│ │   ├─ expected_biog_carbon_kgC		# biogenic carbon mass
│ │   ├─ expected_fossil_carbon_kgC		# fossil carbon mass
│ │   └─ carbon_notes						# notes
│ │
│ └─ pooled_inventory.lock		# Lock file preventing simultaneous writes
│
└─ README.md
```
