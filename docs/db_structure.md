## \# Inventory Database structure



```
inventory\_db/
├─ raw/
│ ├─ showrooms/ 	# Manually curated showroom inventories (Excel)
│ ├─ surveys/ 		# Survey exports (JISC wide format)
│ └─ insurance/ 	# Insurance or property inventory data
│
├─ config/
│  └─ vocab/
│     └─ mapping\_list.xlsx   # List of inventory mappings
│
├─ database/
│ ├─ pooled\_inventory.sqlite 	# Main SQLite database
│ │ ├─ sources
│ │ │ ├─ source\_id \[PK]			# unique source identifier
│ │ │ ├─ data\_source\_type 		# survey / showroom / insurance
│ │ │ ├─ source\_description		# brief description of dataset
│ │ │ ├─ source\_org				# organisation providing data (if applicable)
│ │ │ ├─ file\_name 				# original file name
│ │ │ ├─ file\_path				# local file path
│ │ │ ├─ url					# source URL (if applicable)
│ │ │ ├─ date\_collected 		# date data was originally collected
│ │ │ ├─ date\_imported\_utc 		# timestamp of DB import
│ │ │ └─ notes 					# additional metadata notes
│ │ │
│ │ ├─ inventory\_observations   # item-level inventory observations
│ │ │ ├─ obs\_id \[PK]			# unique observation identifier
│ │ │ ├─ response\_id			# JISC response identifier
│ │ │ ├─ source\_id \[FK]			# link to sources table
│ │ │ ├─ room\_type \[FK]			# room in which item is located
│ │ │ ├─ item\_name \[FK]			# internal item identifier
│ │ │ ├─ count 					# number of items observed
│ │ │ └─ assumption\_notes		# automatic assumptions applied
│ │ │
│ │ ├─ dwelling\_observations	# dwelling-level room count observations
│ │ │ ├─ dwelling\_id \[PK]		# dwelling observation identifier
│ │ │ ├─ response\_id			# response identifier
│ │ │ ├─ source\_id \[FK]			# link to sources table
│ │ │ ├─ room\_type \[FK]			# room that is counted
│ │ │ ├─ count 					# number of rooms observed
│ │ │ └─ assumption\_notes		# automatic assumptions applied
│ │ │
│ │ ├─ survey\_comments			# extracted survey comments
│ │ │ ├─ comment\_obs\_id \[PK]	# comment observation identifier
│ │ │ ├─ response\_id			# response identifier
│ │ │ ├─ source\_id 				# link to sources table
│ │ │ ├─ comment\_type 			# controlled comment category
│ │ │ └─ comment\_text			# comment string (free-text)
│ │ │
│ │ ├─ item\_dictionary			# fixed item vocabulary
│ │ │ ├─ item\_name \[PK]			# internal item identifier
│ │ │ ├─ item\_description 		# user-facing item label
│ │ │ ├─ item\_mass 				# nominal mass (kg)
│ │ │ ├─ price\_search\_term		# search terms to use for price finding
│ │ │ ├─ ons\_price					# ONS pricing (£) if available
│ │ │ ├─ furniture\_class \[FK] 	# associated furniture class
│ │ │ └─ notes 					# item-level notes
│ │ │
│ │ ├─ furniture				# fixed furniture vocabulary
│ │ │ ├─ furniture\_class \[PK]	# furniture class identifier
│ │ │ ├─ furniture\_description	# user-facing class description
│ │ │ ├─ class\_contains			# examples of items in class
│ │ │ ├─ kgC\_kg					# carbon mass per kg item (kgC/kg)
│ │ │ ├─ ratio\_fossil			# fossil carbon fraction
│ │ │ ├─ ratio\_biog				# biogenic carbon fraction
│ │ │ ├─ emission\_factor\_CO2		# emission factor proxy
│ │ │ └─ notes					# class-level notes
│ │ │
│ │ ├─ room						# fixed room vocabulary
│ │ │ ├─ room\_type \[PK]			# internal room identifier
│ │ │ ├─ room\_description		# user-facing room label
│ │ │ ├─ room\_size\_m2			# average room size (m²)
│ │ │ ├─ room\_type\_comp\_1		# room type to compare with
│ │ │ ├─ room\_type\_comp\_2		# room type to compare with
│ │ │ ├─ room\_type\_comp\_ratio	# room comparison size ratio
│ │ │ ├─ size\_assumed			# true / false
│ │ │ ├─ assumption\_notes		# description of assumption
│ │ │ └─ notes					# room-level notes
│ │ │
│ │ ├─ assumed\_inventory		# Assumed household items
│ │ │ ├─ assumed\_item\_id \[PK]	# assumed item row identifier
│ │ │ ├─ room\_type \[FK]			# internal room identifier
│ │ │ ├─ item\_name \[FK]			# internal item identifier
│ │ │ ├─ count\_assumed			# estimated item count
│ │ │ ├─ dependency				# any case dependency
│ │ │ ├─ dependency\_type		# the case type of the dependency
│ │ │ ├─ dependency\_quantifier	# multiplicative qunatifier
│ │ │ └─ assumption\_notes 		# assumption text description
│ │ │
│ │ └─ ingest\_log				# Ingest records for auditing
│ │ │ ├─ ingest\_id \[PK]			# unique ingest run identifier
│ │ │ ├─ source\_id \[FK]			# link to sources table
│ │ │ ├─ data\_source\_type		# type of data ingested
│ │ │ ├─ action					# ingest action performed
│ │ │ ├─ status					# success / failure status
│ │ │ ├─ message				# log message or error summary
│ │ │ ├─ started\_utc			# ingest start timestamp
│ │ │ ├─ finished\_utc			# ingest end timestamp
│ │ │ ├─ rows\_inserted			# number of rows added
│ │ │ └─ rows\_deleted			# number of rows removed
│ │ │
│ │ └─ item\_count\_pmf				# Item count probability mass function
│ │ │ ├─ item\_pmf\_id \[PK]			# unique row identifier
│ │ │ ├─ item\_name	 \[FK]			# item identifier
│ │ │ ├─ room\_type	 \[FK]			# room identifier
│ │ │ ├─ count\_value				# count value identifier
│ │ │ ├─ item\_frequency				# number of occurrences
│ │ │ ├─ item\_probability			# probability of count value
│ │ │ └─ item\_pmf\_notes				# notes
│ │ │
│ │ └─ item\_count\_summary			# Estimated item count
│ │ │ ├─ item\_summary\_id \[PK]		# unique row identifier
│ │ │ ├─ item\_name \[FK]				# item identifier
│ │ │ ├─ room\_type	 \[FK]			# room identifier
│ │ │ ├─ expected\_count\_mean		# computed mean count
│ │ │ ├─ count\_q25					# interpolated 25th percentile
│ │ │ ├─ count\_q75					# interpolated 75th percentile
│ │ │ └─ count\_summary\_notes		# notes
│ │ │
│ │ └─ room\_count\_pmf				# Room count probability mass function
│ │ │ ├─ room\_pmf\_id \[PK]			# unique row identifier
│ │ │ ├─ room\_type	 \[FK]			# room identifier
│ │ │ ├─ count\_value				# count value identifier
│ │ │ ├─ room\_frequency				# number of occurrences
│ │ │ ├─ room\_probability			# probability of count value
│ │ │ └─ room\_pmf\_notes				# notes
│ │ │
│ │ └─ room\_count\_summary			# Estimated room count
│ │ │ ├─ room\_summary\_id \[PK]		# unique row identifier
│ │ │ ├─ room\_type	 \[FK]			# room identifier
│ │ │ ├─ expected\_count\_mean		# computed mean count
│ │ │ ├─ count\_q25					# interpolated 25th percentile
│ │ │ ├─ count\_q75					# interpolated 75th percentile
│ │ │ └─ count\_summary\_notes		# notes
│ │ │
│ │ └─ room\_carbon\_stock				# Estimated carbon stock
│ │ │ ├─ carbon\_summary\_id \[PK]			# unique row identifier
│ │ │ ├─ room\_type \[FK]					# room identifier
│ │ │ ├─ expected\_total\_carbon\_kgC		# total carbon mass
│ │ │ ├─ expected\_biog\_carbon\_kgC		# biogenic carbon mass
│ │ │ ├─ expected\_fossil\_carbon\_kgC		# fossil carbon mass
│ │ │ ├─ q25\_total\_carbon\_kgC			# interpolated q25 equivalent
│ │ │ ├─ q25\_biog\_carbon\_kgC			# interpolated q25 equivalent
│ │ │ ├─ q25\_fossil\_carbon\_kgC			# interpolated q25 equivalent
│ │ │ ├─ q75\_total\_carbon\_kgC			# interpolated q75 equivalent
│ │ │ ├─ q75\_biog\_carbon\_kgC			# interpolated q75 equivalent
│ │ │ ├─ q75\_fossil\_carbon\_kgC			# interpolated q75 equivalent
│ │ │ ├─ carbon\_notes					# notes
│ │ │
│ │ │
│ │ └─ dwelling\_size					# Estimated dwelling size
│ │ │ ├─ dwelling\_type \[PK]				# unique dwelling type identifier
│ │ │ ├─ dwelling\_size\_m2				# dwelling size (m2)
│ │ │ ├─ count\_value					# count for each dwelling type
│ │ │ ├─ dwelling\_type\_pmf				# PMF for each dwelling type
│ │ │ └─ dwelling\_notes					# notes
│ │
│ │ └─ embodied\_carbon\_data				# Spend-based embodied carbon data
│ │   ├─ embodied\_carbon\_id \[PK]		# unique row identifier
│ │   ├─ item\_name \[FK]					# item identifier
│ │   ├─ amazon\_price\_top\_1				# top 10 sold amazon price example
│ │   ├─ amazon\_price\_top\_2				# top 10 sold amazon price example
│ │   ├─ amazon\_price\_top\_3				# top 10 sold amazon price example
│ │   ├─ amazon\_price\_top\_4				# top 10 sold amazon price example
│ │   ├─ amazon\_price\_top\_5				# top 10 sold amazon price example
│ │   ├─ amazon\_price\_top\_6				# top 10 sold amazon price example
│ │   ├─ amazon\_price\_top\_7				# top 10 sold amazon price example
│ │   ├─ amazon\_price\_top\_8				# top 10 sold amazon price example
│ │   ├─ amazon\_price\_top\_9				# top 10 sold amazon price example
│ │   ├─ amazon\_price\_top\_10				# top 10 sold amazon price example
│ │   ├─ amazon\_price\_mean				# mean Amazon price for top 10 sold
│ │   ├─ amazon\_price\_std					# standard deviation of prices
│ │   ├─ amazon\_price\_upper				# upper Amazon price estimate
│ │   ├─ replacement\_cost\_adjusted		# adjusted cost
│ │   ├─ embodied\_CO2\_kg					# spend-based CO2 emission estimate
│ │   └─ notes								# notes
│ │
│ └─ pooled\_inventory.lock		# Lock file preventing simultaneous writes
│
└─ README.md
```



\---



## \# Fire Database structure



```text
fire\_db/
├─ config/
│ ├─ mapping/
│ │ └─ fire\_event\_mappings.xlsm   # Controlled FRIS/fire-event mapping workbook
│ ├─ fire\_input\_param.xlsm         # Controlled single-event input workbook, if used
│ └─ emission\_param.xlsx           # Controlled fire emission parameter workbook
│
├─ raw/
│ ├─ single/                       # Manual/single-event input files, where used
│ └─ fris/                         # FRIS / external fire incident data
│
├─ database/
│ ├─ fire\_incidents.sqlite         # Main SQLite database for fire event modelling
│ │
│ │ ├─ sources                     # General source/import tracking table
│ │ │ ├─ source\_id \[PK]            # unique source identifier
│ │ │ ├─ data\_source\_type          # single / fris / fire\_mappings / emissions / inventory\_snapshot / etc.
│ │ │ ├─ source\_description        # brief description of dataset or workbook
│ │ │ ├─ source\_org                # organisation providing data, if applicable
│ │ │ ├─ file\_name                 # original file name
│ │ │ ├─ file\_path                 # local file path
│ │ │ ├─ url                       # source URL, if applicable
│ │ │ ├─ date\_collected            # date data was originally collected
│ │ │ ├─ date\_imported\_utc         # timestamp of DB import
│ │ │ └─ notes                     # additional metadata notes
│ │ │
│ │ ├─ ingest\_log                  # Ingest/model run records for auditing
│ │ │ ├─ ingest\_id \[PK]            # unique ingest run identifier
│ │ │ ├─ source\_id \[FK]            # link to sources table, where relevant
│ │ │ ├─ data\_source\_type          # type of data ingested or modelled
│ │ │ ├─ action                    # ingest / model / refresh action performed
│ │ │ ├─ status                    # success / failure status
│ │ │ ├─ message                   # log message or error summary
│ │ │ ├─ started\_utc               # ingest/model start timestamp
│ │ │ ├─ finished\_utc              # ingest/model end timestamp
│ │ │ ├─ rows\_inserted             # number of rows added
│ │ │ └─ rows\_deleted              # number of rows removed
│ │ │
│ │ ├─ fire\_emission\_parameter\_mapping    # Fire-category-specific emission model parameters
│ │ │ ├─ parameter\_mapping\_id \[PK]        # unique parameter row identifier
│ │ │ ├─ source\_id \[FK]                   # link to sources table / imported workbook
│ │ │ ├─ fire\_spread\_category             # single\_item / within\_room / multiple\_rooms / entire\_dwelling
│ │ │ ├─ fire\_emission\_parameter          # parameter identifier used by the deterministic fire-impact model
│ │ │ ├─ parameter\_type                   # species\_emission\_factor / model\_control\_parameter
│ │ │ ├─ emission\_species                 # CO2 / CO / future species; NULL for non-species parameters
│ │ │ ├─ ventilation\_condition            # overventilated / underventilated; NULL where not applicable
│ │ │ ├─ is\_applicable                    # 1 if used for this fire category; 0 if blank / N/A in workbook
│ │ │ ├─ value\_min                        # lower sensitivity/testing value
│ │ │ ├─ value\_default                    # deterministic model value
│ │ │ ├─ value\_max                        # upper sensitivity/testing value
│ │ │ ├─ notes                            # user-facing notes copied from workbook
│ │ │ ├─ source\_sheet                     # Excel worksheet name, normally fire\_category\_params
│ │ │ ├─ source\_table                     # source fire-spread-category block in workbook
│ │ │ ├─ input\_row\_number                 # Excel row number
│ │ │ └─ created\_at\_utc                   # timestamp of DB import
│ │ │
│ │ ├─ input\_single\_event                 # Raw/staging rows from fire\_input\_param.xlsm
│ │ │ ├─ staging\_id \[PK]                  # unique staging row identifier
│ │ │ ├─ source\_id \[FK]                   # link to sources table / imported workbook
│ │ │ ├─ input\_row                        # Excel input row number
│ │ │ ├─ fire\_parameter                   # fire input parameter name
│ │ │ ├─ value\_text                       # text input value, where applicable
│ │ │ ├─ value\_numeric                    # numeric input value, where applicable
│ │ │ ├─ value\_bool                       # boolean input value, reserved for future use
│ │ │ ├─ unit                             # input unit, e.g. m2
│ │ │ └─ input\_notes                      # notes copied from workbook, if used
│ │ │
│ │ ├─ input\_bulk\_fris\_events             # Raw/staging rows from FRIS workbook
│ │ │ ├─ source\_id \[FK]                   # link to sources table / imported FRIS workbook
│ │ │ ├─ incident\_id \[PK]                 # FRIS Incident\_Id value, unique per incident
│ │ │ ├─ fiscal\_yr                        # FRIS fiscal year
│ │ │ ├─ property\_type\_3                  # FRIS property type, including occupancy wording where present
│ │ │ ├─ heat\_smoke\_damage\_only           # raw HeatOrSmoke\_Damage\_Only value
│ │ │ ├─ ignition\_source\_all              # raw combined FRIS ignition source/category label
│ │ │ ├─ fire\_size\_on\_arrival             # Fire\_Size\_on\_Arrival value
│ │ │ ├─ fire\_start\_location              # Fire\_Start\_Location value
│ │ │ ├─ item\_first\_ignited               # Item\_First\_Ignited value
│ │ │ ├─ item\_causing\_spread              # Item\_Causing\_Spread value
│ │ │ ├─ extent\_of\_damage                 # Extent\_of\_Damage value
│ │ │ ├─ rapid\_fire\_growth                # Rapid\_Fire\_Growth value
│ │ │ ├─ building\_room\_origin\_size        # Building\_Room\_Origin\_Size value
│ │ │ ├─ building\_floor\_origin\_size       # Building\_Floor\_Origin\_Size value
│ │ │ ├─ building\_fire\_damage\_area        # Building\_Fire\_Damage\_Area value
│ │ │ ├─ building\_total\_damage\_area       # Building\_Total\_Damage\_Area value
│ │ │ └─ distance\_to\_adjoining\_property   # Distance\_to\_Adjoining\_Property value
│ │ │
│ │ ├─ fire\_event\_mapping\_warnings        # Controlled warning catalogue for event resolution
│ │ │ ├─ warning\_type \[PK]                # controlled warning identifier
│ │ │ ├─ warning\_category                 # warning group/category
│ │ │ ├─ warning\_text                     # warning text or template, with optional {placeholders}
│ │ │ ├─ notes                            # warning notes
│ │ │ └─ mapping\_row                      # Excel row number from warnings sheet
│ │ │
│ │ ├─ fire\_event\_mapping\_dwellings       # FRIS Property\_Type\_3 mapped to model dwelling assumptions
│ │ │ ├─ mapping\_id \[PK]                  # unique mapping row identifier
│ │ │ ├─ mapping\_row                      # Excel row number from dwellings sheet
│ │ │ ├─ fris\_dwelling\_naming             # raw FRIS Property\_Type\_3 value
│ │ │ ├─ dwelling\_type                    # reporting/model dwelling type where directly represented
│ │ │ ├─ dwelling\_type\_proxy              # proxy dwelling type used for modelling, if needed
│ │ │ ├─ occupancy\_override               # single / multiple / unknown, where manually specified
│ │ │ ├─ omit\_from\_model                  # 0/1 flag for unsupported dwelling categories
│ │ │ ├─ warning\_type                     # optional warning\_type from warning catalogue
│ │ │ └─ notes                            # mapping notes
│ │ │
│ │ ├─ fire\_event\_mapping\_fire\_cat        # FRIS Extent\_of\_Damage mapped to canonical fire-spread categories
│ │ │ ├─ mapping\_id \[PK]                  # unique mapping row identifier
│ │ │ ├─ mapping\_row                      # Excel row number from fire\_cat sheet
│ │ │ ├─ fris\_fire\_categories             # raw FRIS Extent\_of\_Damage value
│ │ │ ├─ fire\_spread\_category             # Model-facing fire categories
│ │ │ ├─ omit\_from\_model                  # 0/1 flag for categories omitted at this stage
│ │ │ ├─ occupancy\_dependent              # 0/1 flag for categories requiring occupancy/area interpretation
│ │ │ ├─ warning\_type                     # optional warning\_type from warning catalogue
│ │ │ ├─ conditional\_warning              # 0/1 flag; warning emitted only when rule condition is triggered
│ │ │ └─ notes                            # mapping notes
│ │ │
│ │ ├─ fire\_event\_mapping\_items           # FRIS Ignition\_Source\_All mapped to inventory item rules
│ │ │ ├─ mapping\_id \[PK]                  # unique mapping row identifier
│ │ │ ├─ mapping\_row                      # Excel row number from items sheet
│ │ │ ├─ ignition\_source\_all              # raw FRIS Ignition\_Source\_All value
│ │ │ ├─ ignition\_source\_category\_override # optional replacement for parsed ignition source category
│ │ │ ├─ ignition\_source\_override         # optional replacement for parsed ignition source
│ │ │ ├─ single\_item\_status               # instructions on how to deal with single item case
│ │ │ ├─ item\_combusted                   # canonical item\_name for direct/proxy single-item cases
│ │ │ ├─ warning\_type                     # optional warning\_type from warning catalogue
│ │ │ └─ notes                            # mapping notes
│ │ │
│ │ ├─ fire\_event\_mapping\_item\_inference  # Contextual proxy-item rules for conditionally inferred items
│ │ │ ├─ inference\_id \[PK]                # unique inference rule identifier
│ │ │ ├─ mapping\_row                      # Excel row number from item\_inference sheet
│ │ │ ├─ ignition\_category                # parsed/overridden ignition category condition
│ │ │ ├─ ignition\_source                  # parsed/overridden ignition source condition
│ │ │ ├─ fire\_spread\_category             # currently constrained to single\_item
│ │ │ ├─ room\_type                        # optional canonical room\_type condition
│ │ │ ├─ item\_first\_ignited               # optional raw FRIS Item\_First\_Ignited condition
│ │ │ ├─ item\_combusted                   # canonical proxy item\_name returned by the rule
│ │ │ └─ notes                            # inference-rule notes
│ │ │
│ │ ├─ fire\_event\_mapping\_rooms           # FRIS Fire\_Start\_Location mapped to model room\_type
│ │ │ ├─ mapping\_id \[PK]                  # unique mapping row identifier
│ │ │ ├─ mapping\_row                      # Excel row number from rooms sheet
│ │ │ ├─ fire\_start\_location              # raw FRIS Fire\_Start\_Location value
│ │ │ ├─ room\_type                        # canonical room\_type, where mapped
│ │ │ ├─ warning\_type                     # optional warning\_type from warning catalogue
│ │ │ └─ notes                            # mapping notes
│ │ │
│ │ ├─ fire\_event\_mapping\_area\_bands      # Controlled ordering for FRIS damage-area bands
│ │ │ ├─ mapping\_id \[PK]                  # unique mapping row identifier
│ │ │ ├─ mapping\_row                      # Excel row number from area\_bands sheet
│ │ │ ├─ area\_band                        # raw FRIS area-band label
│ │ │ ├─ band\_order                       # ordinal band order used for comparisons
│ │ │ ├─ is\_none\_band                     # 0/1 flag for the "None" band
│ │ │ ├─ is\_open\_ended                    # 0/1 flag for open-ended upper band
│ │ │ └─ notes                            # area-band notes
│ │ │
│ │ ├─ inventory\_snapshot                 # Metadata for the current copied inventory snapshot
│ │ │ ├─ inventory\_snapshot\_id \[PK]       # unique inventory snapshot identifier
│ │ │ ├─ source\_id \[FK]                   # link to sources table
│ │ │ ├─ source\_inventory\_db              # source inventory database path/name
│ │ │ └─ date\_imported\_utc                # timestamp when snapshot was copied
│ │ │
│ │ ├─ inventory\_furniture\_snapshot       # Snapshot of furniture carbon factors
│ │ │ ├─ inventory\_snapshot\_id \[PK, FK]   # inventory snapshot identifier
│ │ │ ├─ furniture\_class \[PK]             # furniture class identifier
│ │ │ ├─ kgC\_kg                           # carbon mass per kg item (kgC/kg)
│ │ │ ├─ ratio\_fossil                     # fossil carbon fraction
│ │ │ └─ ratio\_biog                       # biogenic carbon fraction
│ │ │
│ │ ├─ inventory\_item\_snapshot            # Snapshot of item mass and furniture class
│ │ │ ├─ inventory\_snapshot\_id \[PK, FK]   # inventory snapshot identifier
│ │ │ ├─ item\_name \[PK]                   # canonical item identifier
│ │ │ ├─ item\_mass\_kg                     # nominal item mass (kg)
│ │ │ └─ furniture\_class \[FK]             # associated furniture class
│ │ │
│ │ ├─ inventory\_room\_snapshot            # Fire-facing room lookup with size, count and carbon stock
│ │ │ ├─ inventory\_snapshot\_id \[PK, FK]   # inventory snapshot identifier
│ │ │ ├─ room\_type \[PK]                   # canonical room identifier
│ │ │ ├─ room\_description                 # user-facing room label
│ │ │ ├─ room\_size\_m2                     # average / assumed room size (m²)
│ │ │ ├─ expected\_count\_mean              # expected room count
│ │ │ ├─ count\_q25                        # 25th percentile room count
│ │ │ ├─ count\_q75                        # 75th percentile room count
│ │ │ ├─ expected\_total\_carbon\_kgC        # expected total room carbon stock
│ │ │ ├─ expected\_biog\_carbon\_kgC         # expected biogenic room carbon stock
│ │ │ ├─ expected\_fossil\_carbon\_kgC       # expected fossil room carbon stock
│ │ │ ├─ q25\_total\_carbon\_kgC             # q25 total room carbon stock
│ │ │ ├─ q25\_biog\_carbon\_kgC              # q25 biogenic room carbon stock
│ │ │ ├─ q25\_fossil\_carbon\_kgC            # q25 fossil room carbon stock
│ │ │ ├─ q75\_total\_carbon\_kgC             # q75 total room carbon stock
│ │ │ ├─ q75\_biog\_carbon\_kgC              # q75 biogenic room carbon stock
│ │ │ └─ q75\_fossil\_carbon\_kgC            # q75 fossil room carbon stock
│ │ │
│ │ ├─ inventory\_dwelling\_size\_snapshot   # Snapshot of dwelling size and dwelling-type PMF
│ │ │ ├─ inventory\_snapshot\_id \[PK, FK]   # inventory snapshot identifier
│ │ │ ├─ dwelling\_type \[PK]               # canonical dwelling type
│ │ │ ├─ dwelling\_size\_m2                 # dwelling size (m²)
│ │ │ ├─ count\_value                      # observed/source count for this dwelling type
│ │ │ └─ dwelling\_type\_pmf                # dwelling type probability mass
│ │ │
│ │ ├─ fire\_events                    		# Resolved/model-facing fire event records
│ │ │ ├─ event\_id \[PK] 				# Internal auto-incrementing model-facing event ID

│ │ │ ├─ source\_id \[FK]				# event identifier copied from staged input source

│ │ │ ├─ incident\_id 				# Original/source event identifier, e.g. FRIS Incident\_Id

│ │ │ ├─ input\_type 				# Input route: fris / scenario / single (legacy)

│ │ │ ├─ inventory\_snapshot\_id \[FK] 		# Inventory snapshot used for resolution/model inputs

│ │ │ ├─ fiscal\_year\_start 			# Start year of FRIS fiscal year, e.g. 2022 for 2022/23

│ │ │ ├─ fiscal\_year\_end 			# End year of FRIS fiscal year, e.g. 2023 for 2022/23

│ │ │ ├─ property\_type\_3\_input 			# Raw FRIS Property\_Type\_3 value

│ │ │ ├─ dwelling\_type 				# Resolved/reporting dwelling type

│ │ │ ├─ dwelling\_type\_proxy 			# Optional proxy dwelling type used for model assumptions

│ │ │ ├─ dwelling\_type\_for\_model 		# dwelling\_type\_proxy if present, else dwelling\_type

│ │ │ ├─ occupancy 				# Resolved occupancy class: single / multiple / unknown

│ │ │ ├─ heat\_smoke\_damage\_only\_input 		# Raw/resolved boolean flag from FRIS heat/smoke-only field

│ │ │ ├─ extent\_of\_damage\_input 	 	# Raw FRIS Extent\_of\_Damage value

│ │ │ ├─ fire\_spread\_category\_from\_extent 	# Category resolved directly from Extent\_of\_Damage

│ │ │ ├─ fire\_spread\_category 			# Final resolved fire spread category used by the model

│ │ │ ├─ fire\_start\_location\_input 		# Raw FRIS Fire\_Start\_Location value

│ │ │ ├─ room\_of\_origin 			# Resolved canonical inventory room\_type

│ │ │ ├─ building\_fire\_damage\_area\_input 	# Raw FRIS Building\_Fire\_Damage\_Area band

│ │ │ ├─ building\_fire\_damage\_area\_band\_index 	# Numeric index for the resolved fire-damage area band

│ │ │ ├─ building\_total\_damage\_area\_input 	# Raw FRIS Building\_Total\_Damage\_Area band

│ │ │ ├─ building\_total\_damage\_area\_band\_index 	# Numeric index for the resolved total-damage area band

│ │ │ ├─ building\_room\_origin\_size\_input 	# Raw FRIS Building\_Room\_Origin\_Size value/band

│ │ │ ├─ building\_floor\_origin\_size\_input 	# Raw FRIS Building\_Floor\_Origin\_Size value/band

│ │ │ ├─ ignition\_source\_all\_input 		# Raw FRIS Ignition\_Source\_All value

│ │ │ ├─ ignition\_source\_category\_input 	# Parsed category from ignition\_source\_all\_input

│ │ │ ├─ ignition\_source\_input 			# Parsed source from ignition\_source\_all\_input

│ │ │ ├─ ignition\_source\_category 		# Resolved ignition source category after overrides

│ │ │ ├─ ignition\_source 			# Resolved ignition source after overrides

│ │ │ ├─ item\_first\_ignited\_input 		# Raw FRIS Item\_First\_Ignited value

│ │ │ ├─ item\_causing\_spread\_input 		# Raw FRIS Item\_Causing\_Spread value, if available

│ │ │ ├─ single\_item\_status 			# Mapping status for single-item cases

│ │ │ ├─ item\_combusted 			# Resolved inventory item\_name for single-item cases

│ │ │ ├─ omit\_from\_model 			# 0 = included, 1 = retained but excluded from model calculation

│ │ │ ├─ omit\_reason 				# Main reason for omission, if omit\_from\_model = 1

│ │ │ ├─ data\_quality\_status 			# included / included\_with\_warning / omitted

│ │ │ ├─ suspicious\_fields 			# Optional summary of suspicious/conflicting fields
│ │ │ └─ resolution\_notes                      	# Free-text notes from event resolution
│ │ │
│ │ ├─ fire\_event\_warnings                # Structured warnings generated during fire event resolution
│ │ │ ├─ warning\_id \[PK]                  # unique warning identifier

│ │ │ ├─ event\_id \[FK]			  # link to fire\_events.event\_id
│ │ │ ├─ incident\_id \[FK]                 # link to fire\_events.source\_id
│ │ │ ├─ warning\_type                     # controlled warning type
│ │ │ ├─ warning\_severity                 # info / warning / model\_assumption
│ │ │ ├─ fire\_parameter                   # related fire parameter, where applicable
│ │ │ └─ warning\_message                  # human-readable warning text
│ │ │
│ │ └─ v\_inventory\_item\_carbon\_lookup     # View: item-level carbon lookup for single-item calculations
│ │   ├─ inventory\_snapshot\_id            # inventory snapshot identifier
│ │   ├─ item\_name                        # canonical item identifier
│ │   ├─ item\_mass\_kg                     # nominal item mass (kg)
│ │   ├─ furniture\_class                  # associated furniture class
│ │   ├─ kgC\_kg                           # carbon mass per kg item (kgC/kg)
│ │   ├─ ratio\_fossil                     # fossil carbon fraction
│ │   ├─ ratio\_biog                       # biogenic carbon fraction
│ │   ├─ item\_total\_carbon\_kgC            # item\_mass\_kg \* kgC\_kg
│ │   ├─ item\_biog\_carbon\_kgC             # item\_total\_carbon\_kgC \* ratio\_biog
│ │   └─ item\_fossil\_carbon\_kgC           # item\_total\_carbon\_kgC \* ratio\_fossil
│ │
│ └─ fire\_incidents.lock                  # Lock file preventing simultaneous writes, when active
│
└─ README.md
```

