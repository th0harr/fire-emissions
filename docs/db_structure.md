
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
│ │ │ ├─ obs_id [PK]			# unique observation identifier
│ │ │ ├─ response_id			# JISC response identifier
│ │ │ ├─ source_id [FK]			# link to sources table
│ │ │ ├─ room_type [FK]			# room in which item is located
│ │ │ ├─ item_name [FK]			# internal item identifier
│ │ │ ├─ count 					# number of items observed
│ │ │ └─ assumption_notes		# automatic assumptions applied
│ │ │
│ │ ├─ dwelling_observations	# dwelling-level room count observations
│ │ │ ├─ dwelling_id [PK]		# dwelling observation identifier
│ │ │ ├─ response_id			# response identifier
│ │ │ ├─ source_id [FK]			# link to sources table
│ │ │ ├─ room_type [FK]			# room that is counted
│ │ │ ├─ count 					# number of rooms observed
│ │ │ └─ assumption_notes		# automatic assumptions applied
│ │ │
│ │ ├─ survey_comments			# extracted survey comments
│ │ │ ├─ comment_obs_id [PK]	# comment observation identifier
│ │ │ ├─ response_id			# response identifier
│ │ │ ├─ source_id 				# link to sources table
│ │ │ ├─ comment_type 			# controlled comment category
│ │ │ └─ comment_text			# comment string (free-text)
│ │ │
│ │ ├─ item_dictionary			# fixed item vocabulary
│ │ │ ├─ item_name [PK]			# internal item identifier
│ │ │ ├─ item_description 		# user-facing item label
│ │ │ ├─ item_mass 				# nominal mass (kg)
│ │ │ ├─ price_search_term		# search terms to use for price finding
│ │ │ ├─ ons_price					# ONS pricing (£) if available
│ │ │ ├─ furniture_class [FK] 	# associated furniture class
│ │ │ └─ notes 					# item-level notes
│ │ │
│ │ ├─ furniture				# fixed furniture vocabulary
│ │ │ ├─ furniture_class [PK]	# furniture class identifier
│ │ │ ├─ furniture_description	# user-facing class description
│ │ │ ├─ class_contains			# examples of items in class
│ │ │ ├─ kgC_kg					# carbon mass per kg item (kgC/kg)
│ │ │ ├─ ratio_fossil			# fossil carbon fraction
│ │ │ ├─ ratio_biog				# biogenic carbon fraction
│ │ │ ├─ emission_factor_CO2		# emission factor proxy
│ │ │ └─ notes					# class-level notes
│ │ │
│ │ ├─ room						# fixed room vocabulary
│ │ │ ├─ room_type [PK]			# internal room identifier
│ │ │ ├─ room_description		# user-facing room label
│ │ │ ├─ room_size_m2			# average room size (m²)
│ │ │ ├─ room_type_comp_1		# room type to compare with
│ │ │ ├─ room_type_comp_2		# room type to compare with
│ │ │ ├─ room_type_comp_ratio	# room comparison size ratio
│ │ │ ├─ size_assumed			# true / false
│ │ │ ├─ assumption_notes		# description of assumption
│ │ │ └─ notes					# room-level notes
│ │ │
│ │ ├─ assumed_inventory		# Assumed household items
│ │ │ ├─ assumed_item_id [PK]	# assumed item row identifier
│ │ │ ├─ room_type [FK]			# internal room identifier
│ │ │ ├─ item_name [FK]			# internal item identifier
│ │ │ ├─ count_assumed			# estimated item count
│ │ │ ├─ dependency				# any case dependency
│ │ │ ├─ dependency_type		# the case type of the dependency
│ │ │ ├─ dependency_quantifier	# multiplicative qunatifier
│ │ │ └─ assumption_notes 		# assumption text description
│ │ │
│ │ └─ ingest_log				# Ingest records for auditing
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
│ │ │ ├─ item_pmf_id [PK]			# unique row identifier
│ │ │ ├─ item_name	 [FK]			# item identifier
│ │ │ ├─ room_type	 [FK]			# room identifier
│ │ │ ├─ count_value				# count value identifier
│ │ │ ├─ item_frequency				# number of occurrences
│ │ │ ├─ item_probability			# probability of count value
│ │ │ └─ item_pmf_notes				# notes
│ │ │
│ │ └─ item_count_summary			# Estimated item count
│ │ │ ├─ item_summary_id [PK]		# unique row identifier
│ │ │ ├─ item_name [FK]				# item identifier
│ │ │ ├─ room_type	 [FK]			# room identifier
│ │ │ ├─ expected_count_mean		# computed mean count
│ │ │ ├─ count_q25					# interpolated 25th percentile
│ │ │ ├─ count_q75					# interpolated 75th percentile
│ │ │ └─ count_summary_notes		# notes
│ │ │
│ │ └─ room_count_pmf				# Room count probability mass function
│ │ │ ├─ room_pmf_id [PK]			# unique row identifier
│ │ │ ├─ room_type	 [FK]			# room identifier
│ │ │ ├─ count_value				# count value identifier
│ │ │ ├─ room_frequency				# number of occurrences
│ │ │ ├─ room_probability			# probability of count value
│ │ │ └─ room_pmf_notes				# notes
│ │ │
│ │ └─ room_count_summary			# Estimated room count
│ │ │ ├─ room_summary_id [PK]		# unique row identifier
│ │ │ ├─ room_type	 [FK]			# room identifier
│ │ │ ├─ expected_count_mean		# computed mean count
│ │ │ ├─ count_q25					# interpolated 25th percentile
│ │ │ ├─ count_q75					# interpolated 75th percentile
│ │ │ └─ count_summary_notes		# notes
│ │ │
│ │ └─ room_carbon_stock				# Estimated carbon stock
│ │ │ ├─ carbon_summary_id [PK]			# unique row identifier
│ │ │ ├─ room_type [FK]					# room identifier
│ │ │ ├─ expected_total_carbon_kgC		# total carbon mass
│ │ │ ├─ expected_biog_carbon_kgC		# biogenic carbon mass
│ │ │ ├─ expected_fossil_carbon_kgC		# fossil carbon mass
│ │ │ ├─ q25_total_carbon_kgC			# interpolated q25 equivalent
│ │ │ ├─ q25_biog_carbon_kgC			# interpolated q25 equivalent
│ │ │ ├─ q25_fossil_carbon_kgC			# interpolated q25 equivalent
│ │ │ ├─ q75_total_carbon_kgC			# interpolated q75 equivalent
│ │ │ ├─ q75_biog_carbon_kgC			# interpolated q75 equivalent
│ │ │ ├─ q75_fossil_carbon_kgC			# interpolated q75 equivalent
│ │ │ ├─ carbon_notes					# notes
│ │ │
│ │ │
│ │ └─ dwelling_size					# Estimated dwelling size
│ │ │ ├─ dwelling_type [PK]				# unique dwelling type identifier
│ │ │ ├─ dwelling_size_m2				# dwelling size (m2)
│ │ │ ├─ count_value					# count for each dwelling type
│ │ │ ├─ dwelling_type_pmf				# PMF for each dwelling type
│ │ │ └─ dwelling_notes					# notes
│ │
│ │ └─ embodied_carbon_data				# Spend-based embodied carbon data
│ │   ├─ embodied_carbon_id [PK]		# unique row identifier
│ │   ├─ item_name [FK]					# item identifier
│ │   ├─ amazon_price_top_1				# top 10 sold amazon price example
│ │   ├─ amazon_price_top_2				# top 10 sold amazon price example
│ │   ├─ amazon_price_top_3				# top 10 sold amazon price example
│ │   ├─ amazon_price_top_4				# top 10 sold amazon price example
│ │   ├─ amazon_price_top_5				# top 10 sold amazon price example
│ │   ├─ amazon_price_top_6				# top 10 sold amazon price example
│ │   ├─ amazon_price_top_7				# top 10 sold amazon price example
│ │   ├─ amazon_price_top_8				# top 10 sold amazon price example
│ │   ├─ amazon_price_top_9				# top 10 sold amazon price example
│ │   ├─ amazon_price_top_10				# top 10 sold amazon price example
│ │   ├─ amazon_price_mean				# mean Amazon price for top 10 sold
│ │   ├─ amazon_price_std					# standard deviation of prices
│ │   ├─ amazon_price_upper				# upper Amazon price estimate
│ │   ├─ replacement_cost_adjusted		# adjusted cost
│ │   ├─ embodied_CO2_kg					# spend-based CO2 emission estimate
│ │   └─ notes								# notes
│ │
│ └─ pooled_inventory.lock		# Lock file preventing simultaneous writes
│
└─ README.md
```

## Fire Database structure

```text
fire_db/
├─ config/
│ ├─ fire_input_param.xlsm       # Controlled fire event input workbook
│ └─ emission_param.xlsx         # Controlled fire emission parameter workbook
│
├─ raw/
│ └─ fris/                       # FRIS / external fire incident data, currently unused
│
├─ database/
│ ├─ fire_incidents.sqlite       # Main SQLite database for fire event modelling
│ │
│ │ ├─ sources                   # General source/import tracking table
│ │ │ ├─ source_id [PK]          # unique source identifier
│ │ │ ├─ data_source_type        # fire_event / emissions / inventory_snapshot / etc.
│ │ │ ├─ source_description      # brief description of dataset or workbook
│ │ │ ├─ source_org              # organisation providing data, if applicable
│ │ │ ├─ file_name               # original file name
│ │ │ ├─ file_path               # local file path
│ │ │ ├─ url                     # source URL, if applicable
│ │ │ ├─ date_collected          # date data was originally collected
│ │ │ ├─ date_imported_utc       # timestamp of DB import
│ │ │ └─ notes                   # additional metadata notes
│ │ │
│ │ ├─ ingest_log                # Ingest/model run records for auditing
│ │ │ ├─ ingest_id [PK]          # unique ingest run identifier
│ │ │ ├─ source_id [FK]          # link to sources table, where relevant
│ │ │ ├─ data_source_type        # type of data ingested or modelled
│ │ │ ├─ action                  # ingest / model / refresh action performed
│ │ │ ├─ status                  # success / failure status
│ │ │ ├─ message                 # log message or error summary
│ │ │ ├─ started_utc             # ingest/model start timestamp
│ │ │ ├─ finished_utc            # ingest/model end timestamp
│ │ │ ├─ rows_inserted           # number of rows added
│ │ │ └─ rows_deleted            # number of rows removed
│ │ │
│ │ ├─ input_single_event        		   # Raw/staging rows from fire_input_param.xlsm
│ │ │ ├─ staging_id [PK]                 # unique staging row identifier
│ │ │ ├─ source_id [FK]                  # link to sources table / imported workbook
│ │ │ ├─ input_row                       # Excel input row number
│ │ │ ├─ fire_parameter                  # fire input parameter name
│ │ │ ├─ value_text                      # text input value, where applicable
│ │ │ ├─ value_numeric                   # numeric input value, where applicable
│ │ │ ├─ value_bool                      # boolean input value, reserved for future use
│ │ │ ├─ unit                            # input unit, e.g. m2
│ │ │ └─ input_notes                     # notes copied from workbook, if used
│ │ │
│ │ ├─ input_bulk_fris_events            # Raw/staging rows from fris_raw.xlsx
│ │ │ ├─ source_id [FK]                  # link to sources table / imported FRIS workbook
│ │ │ ├─ incident_id [PK]                # FRIS Incident_Id value, unique per incident
│ │ │ ├─ fiscal_yr                       # FRIS fiscal year
│ │ │ ├─ property_type_3                 # FRIS property type (plus occupancy)
│ │ │ ├─ heat_smoke_damage_only          # raw HeatOrSmoke_Damage_Only value
│ │ │ ├─ ignition_source_all             # raw combined FRIS ignition source/category label
│ │ │ ├─ fire_size_on_arrival            # Fire_Size_on_Arrival value
│ │ │ ├─ fire_start_location             # Fire_Start_Location value
│ │ │ ├─ item_first_ignited              # Item_First_Ignited value
│ │ │ ├─ item_causing_spread             # Item_Causing_Spread value
│ │ │ ├─ extent_of_damage                # Extent_of_Damage value
│ │ │ ├─ rapid_fire_growth               # Rapid_Fire_Growth value
│ │ │ ├─ building_room_origin_size       # Building_Room_Origin_Size value
│ │ │ ├─ building_floor_origin_size      # Building_Floor_Origin_Size value
│ │ │ ├─ building_fire_damage_area       # Building_Fire_Damage_Area value
│ │ │ ├─ building_total_damage_area      # Building_Total_Damage_Area
│ │ │ └─ distance_to_adjoining_property  # Distance_to_Adjoining_Property value
│ │ │                                    
│ │ ├─ fire_input_value_mapping          # User-facing fire input names mapped to canonical values
│ │ │ ├─ mapping_id [PK]                 # unique mapping row identifier
│ │ │ ├─ mapping_row                     # Excel row number from input_mapping sheet
│ │ │ ├─ input_value                     # user-facing workbook value
│ │ │ ├─ canonical_value                 # canonical model-facing value
│ │ │ └─ name_category                   # parameter/category to which the mapping applies
│ │ │
│ │ ├─ fire_ignition_item_mapping        # FRIS ignition source mapped to inventory item
│ │ │ ├─ mapping_id [PK]                 # unique mapping row identifier
│ │ │ ├─ mapping_row                     # Excel row number from item_mapping sheet
│ │ │ ├─ ignition_source                 # FRIS ignition source label
│ │ │ ├─ ignition_source_category        # FRIS ignition source category
│ │ │ ├─ single_item_status              # direct_inventory_item / proxy_inventory_item / invalid_single_item / unmapped
│ │ │ ├─ item_combusted                  # canonical item_name used for single_item cases
│ │ │ └─ mapping_notes                   # mapping notes
│ │ │
│ │ ├─ fire_emission_parameter_mapping   # Fire-category-specific emission model parameters
│ │ │ ├─ parameter_mapping_id [PK]       # unique parameter row identifier
│ │ │ ├─ source_id [FK]                  # link to sources table / imported workbook
│ │ │ ├─ fire_spread_category            # single_item / within_room / multiple_rooms / entire_dwelling
│ │ │ ├─ fire_emission_parameter         # parameter identifier used by the deterministic fire-impact model
│ │ │ ├─ parameter_type                  # species_emission_factor / model_control_parameter
│ │ │ ├─ emission_species                # CO2 / CO / future species; NULL for non-species parameters
│ │ │ ├─ ventilation_condition           # overventilated / underventilated; NULL where not applicable
│ │ │ ├─ is_applicable                   # 1 if used for this fire category; 0 if blank / N/A in workbook
│ │ │ ├─ value_min                       # lower sensitivity/testing value
│ │ │ ├─ value_default                   # deterministic model value
│ │ │ ├─ value_max                       # upper sensitivity/testing value
│ │ │ ├─ notes                           # user-facing notes copied from workbook
│ │ │ ├─ source_sheet                    # Excel worksheet name, normally fire_category_params
│ │ │ ├─ source_table                    # source fire-spread-category block in workbook
│ │ │ ├─ input_row_number                # Excel row number
│ │ │ └─ created_at_utc                  # timestamp of DB import
│ │ │
│ │ ├─ inventory_snapshot                # Metadata for the current copied inventory snapshot
│ │ │ ├─ inventory_snapshot_id [PK]      # unique inventory snapshot identifier
│ │ │ ├─ source_id [FK]                  # link to sources table
│ │ │ ├─ source_inventory_db             # source inventory database path/name
│ │ │ └─ date_imported_utc               # timestamp when snapshot was copied
│ │ │
│ │ ├─ inventory_furniture_snapshot      # Snapshot of furniture carbon factors
│ │ │ ├─ inventory_snapshot_id [PK, FK]  # inventory snapshot identifier
│ │ │ ├─ furniture_class [PK]            # furniture class identifier
│ │ │ ├─ kgC_kg                          # carbon mass per kg item (kgC/kg)
│ │ │ ├─ ratio_fossil                    # fossil carbon fraction
│ │ │ └─ ratio_biog                      # biogenic carbon fraction
│ │ │
│ │ ├─ inventory_item_snapshot           # Snapshot of item mass and furniture class
│ │ │ ├─ inventory_snapshot_id [PK, FK]  # inventory snapshot identifier
│ │ │ ├─ item_name [PK]                  # canonical item identifier
│ │ │ ├─ item_mass_kg                    # nominal item mass (kg)
│ │ │ └─ furniture_class [FK]            # associated furniture class
│ │ │
│ │ ├─ inventory_room_snapshot           # Fire-facing room lookup with size, count and carbon stock
│ │ │ ├─ inventory_snapshot_id [PK, FK]  # inventory snapshot identifier
│ │ │ ├─ room_type [PK]                  # canonical room identifier
│ │ │ ├─ room_description                # user-facing room label
│ │ │ ├─ room_size_m2                    # average / assumed room size (m²)
│ │ │ ├─ expected_count_mean             # expected room count
│ │ │ ├─ count_q25                       # 25th percentile room count
│ │ │ ├─ count_q75                       # 75th percentile room count
│ │ │ ├─ expected_total_carbon_kgC       # expected total room carbon stock
│ │ │ ├─ expected_biog_carbon_kgC        # expected biogenic room carbon stock
│ │ │ ├─ expected_fossil_carbon_kgC      # expected fossil room carbon stock
│ │ │ ├─ q25_total_carbon_kgC            # q25 total room carbon stock
│ │ │ ├─ q25_biog_carbon_kgC             # q25 biogenic room carbon stock
│ │ │ ├─ q25_fossil_carbon_kgC           # q25 fossil room carbon stock
│ │ │ ├─ q75_total_carbon_kgC            # q75 total room carbon stock
│ │ │ ├─ q75_biog_carbon_kgC             # q75 biogenic room carbon stock
│ │ │ └─ q75_fossil_carbon_kgC           # q75 fossil room carbon stock
│ │ │
│ │ ├─ inventory_dwelling_size_snapshot  # Snapshot of dwelling size and dwelling-type PMF
│ │ │ ├─ inventory_snapshot_id [PK, FK]  # inventory snapshot identifier
│ │ │ ├─ dwelling_type [PK]              # canonical dwelling type
│ │ │ ├─ dwelling_size_m2                # dwelling size (m²)
│ │ │ ├─ count_value                     # observed/source count for this dwelling type
│ │ │ └─ dwelling_type_pmf               # dwelling type probability mass
│ │ │
│ │ ├─ fire_events                       # Resolved/model-facing fire event records
│ │ │ ├─ source_id [PK]                  # event identifier copied from staged input source
│ │ │ ├─ inventory_snapshot_id           # inventory snapshot used for resolution/model inputs
│ │ │ ├─ fire_spread_category_input      # original user-facing fire spread input
│ │ │ ├─ fire_spread_category            # heat_smoke / single_item / within_room / multiple_rooms / entire_dwelling
│ │ │ ├─ room_of_origin_input            # original room input label
│ │ │ ├─ room_of_origin                  # resolved canonical room_type
│ │ │ ├─ fire_area_m2                    # directly burned/fire-damaged area (m²)
│ │ │ ├─ smoke_heat_damage_area_m2       # smoke/heat damaged replacement area (m²)
│ │ │ ├─ room_of_origin_size_m2          # case-specific or defaulted room size (m²)
│ │ │ ├─ dwelling_size_m2                # case-specific or defaulted dwelling size (m²)
│ │ │ ├─ dwelling_type_input             # original dwelling type input label
│ │ │ ├─ dwelling_type                   # resolved canonical dwelling type
│ │ │ ├─ ignition_source                 # FRIS/user ignition source label
│ │ │ ├─ single_item_status              # item mapping status for single_item cases
│ │ │ ├─ item_combusted                  # resolved item_name for single_item cases
│ │ │ ├─ resolution_notes                # text notes from input resolution
│ │ │ └─ created_at_utc                  # timestamp when event row was created
│ │ │
│ │ ├─ fire_event_warnings               # Structured warnings generated during fire event resolution
│ │ │ ├─ warning_id [PK]                 # unique warning identifier
│ │ │ ├─ source_id [FK]                  # link to fire_events.source_id
│ │ │ ├─ warning_type                    # controlled warning type
│ │ │ ├─ warning_severity                # info / warning / model_assumption
│ │ │ ├─ fire_parameter                  # related fire parameter, where applicable
│ │ │ ├─ warning_message                 # human-readable warning text
│ │ │ └─ created_at_utc                  # timestamp when warning was generated
│ │ │
│ │ └─ v_inventory_item_carbon_lookup    # View: item-level carbon lookup for single-item calculations
│ │   ├─ inventory_snapshot_id           # inventory snapshot identifier
│ │   ├─ item_name                       # canonical item identifier
│ │   ├─ item_mass_kg                    # nominal item mass (kg)
│ │   ├─ furniture_class                 # associated furniture class
│ │   ├─ kgC_kg                          # carbon mass per kg item (kgC/kg)
│ │   ├─ ratio_fossil                    # fossil carbon fraction
│ │   ├─ ratio_biog                      # biogenic carbon fraction
│ │   ├─ item_total_carbon_kgC           # item_mass_kg * kgC_kg
│ │   ├─ item_biog_carbon_kgC            # item_total_carbon_kgC * ratio_biog
│ │   └─ item_fossil_carbon_kgC          # item_total_carbon_kgC * ratio_fossil
│ │
│ └─ fire_incidents.lock                 # Lock file preventing simultaneous writes, when active
│
└─ README.md
```

