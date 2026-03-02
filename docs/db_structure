
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
│ │ ├─ inventory_observations
│ │ │ ├─ obs_id [PK]			# unique observation identifier
│ │ │ ├─ source_id 				# link to sources table
│ │ │ ├─ room_type 				# room in which item is located
│ │ │ ├─ item_description 		# curated descriptive label
│ │ │ ├─ item_name 				# internal item identifier
│ │ │ ├─ count 					# number of items observed
│ │ │ ├─ furniture_class 		# grouping for emissions modelling
│ │ │ └─ notes 					# observation-specific notes
│ │ │
│ │ ├─ item_dictionary
│ │ │ ├─ item_name [PK]			# internal item identifier
│ │ │ ├─ item_description 		# user-facing item label
│ │ │ ├─ item_mass 				# nominal mass (kg)
│ │ │ ├─ furniture_class 		# associated furniture class
│ │ │ └─ notes 					# item-level notes
│ │ │
│ │ ├─ furniture_class
│ │ │ ├─ furniture_class [PK]	# furniture class identifier
│ │ │ ├─ furniture_description	# user-facing class description
│ │ │ ├─ class_contains			# examples of items in class
│ │ │ ├─ kgC_kg					# carbon mass per kg item
│ │ │ ├─ ratio_fossil			# fossil carbon fraction
│ │ │ ├─ ratio_biog				# biogenic carbon fraction
│ │ │ └─ notes					# class-level notes
│ │ │
│ │ ├─ room_type
│ │ │ ├─ room_type [PK]			# room identifier
│ │ │ └─ notes					# room-level notes
│ │ │
│ │ └─ ingest_log
│ │   ├─ ingest_id [PK]			# unique ingest run identifier
│ │   ├─ data_source_type		# type of data ingested
│ │   ├─ action					# ingest action performed
│ │   ├─ status					# success / failure status
│ │   ├─ message				# log message or error summary
│ │   ├─ started_utc			# ingest start timestamp
│ │   ├─ finished_utc			# ingest end timestamp
│ │   ├─ rows_inserted			# number of rows added
│ │   └─ rows_deleted			# number of rows removed
│ │
│ └─ pooled_inventory.lock		# Lock file preventing simultaneous writes
│
└─ README.md
```
