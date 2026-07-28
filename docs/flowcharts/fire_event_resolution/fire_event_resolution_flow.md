\# Fire event resolution flow chart v3



```mermaid

flowchart TD

&#x20;   %% =====================================================================

&#x20;   %% START / GLOBAL PRECHECKS

&#x20;   %% =====================================================================

&#x20;   START(\[Start FRIS build]) --> LOAD\[Load FRIS staging rows\\nLoad fire\_event\_mappings workbook]

&#x20;   LOAD --> PRECHECK\[Run build-level mapping/schema prechecks]



&#x20;   PRECHECK --> PC1{Mapping workbook structurally valid?}

&#x20;   PC1 -->|no| BLOCK\_SCHEMA\[\[Blocking build error\\nInvalid mapping workbook schema]]

&#x20;   PC1 -->|yes| PC2{All present raw categories covered\\nby required mapping tables?}



&#x20;   PC2 -->|no| BLOCK\_COVERAGE\[\[Blocking build error\\nPresent FRIS category missing from mapping table]]

&#x20;   PC2 -->|yes| ROW\_START(\[Start next FRIS incident row])



&#x20;   %% =====================================================================

&#x20;   %% ROW NORMALISATION AND REQUIRED RAW INPUT VALIDATION

&#x20;   %% =====================================================================

&#x20;   ROW\_START --> NORM\[Normalise raw FRIS values]

&#x20;   NORM --> REQ{Required raw inputs usable?}



&#x20;   REQ -->|property\_type\_3 missing / blank / NULL / NaN| OMIT\_REQUIRED\[Omit incident row\\nwarning\_type = missing\_required\_fris\_field\\nwarning\_severity = omit\_row]

&#x20;   REQ -->|extent\_of\_damage missing / blank / NULL / NaN| OMIT\_REQUIRED

&#x20;   REQ -->|fire\_start\_location missing / blank / NULL / NaN| OMIT\_REQUIRED

&#x20;   REQ -->|yes| HS\_NORM\[Normalise heat\_smoke\_damage\_only\\nAllowed resolved values: yes / no / NULL]



&#x20;   HS\_NORM --> HS\_VAL{heat\_smoke\_damage\_only value}

&#x20;   HS\_VAL -->|NULL| OMIT\_HEATSMOKE\[Omit incident row\\nwarning\_type = missing\_required\_fris\_field\\nfield = heat\_smoke\_damage\_only\\nwarning\_severity = omit\_row]

&#x20;   HS\_VAL -->|yes| ROUTE\_SPLIT

&#x20;   HS\_VAL -->|no| ROUTE\_SPLIT



&#x20;   %% =====================================================================

&#x20;   %% BRANCH INTO SEPARATE RESOLUTION ROUTES

&#x20;   %% =====================================================================

&#x20;   ROUTE\_SPLIT{{Resolve incident through separate routes}}



&#x20;   ROUTE\_SPLIT --> DWELL\_START

&#x20;   ROUTE\_SPLIT --> AREA\_START

&#x20;   ROUTE\_SPLIT --> FIRE\_START

&#x20;   ROUTE\_SPLIT --> ROOM\_START

&#x20;   ROUTE\_SPLIT --> ITEM\_START



&#x20;   %% =====================================================================

&#x20;   %% DWELLING ROUTE

&#x20;   %% =====================================================================

&#x20;   subgraph DWELLING\[Resolve dwelling route]

&#x20;       direction TD

&#x20;       DWELL\_START\[Use property\_type\_3] --> DW1{property\_type\_3 in\\nfire\_event\_mapping\_dwellings?}

&#x20;       DW1 -->|no| BLOCK\_DWELL\[\[Blocking build error\\nDwelling mapping incomplete]]

&#x20;       DW1 -->|yes| DW2\[Resolve dwelling\_type\\ndwelling\_type\_proxy\\noccupancy]



&#x20;       DW2 --> DW3{Mapping says omit\_row?}

&#x20;       DW3 -->|yes| DW\_OMIT\[Set row outcome = omit\\nAppend configured dwelling warning]

&#x20;       DW3 -->|no| DW4{dwelling\_type\_proxy populated?}



&#x20;       DW4 -->|yes| DW5\[Append warning\_type\\nsubstitute\_proxy\_dwelling\_type\\nInsert dwelling\_type and dwelling\_type\_proxy into message]

&#x20;       DW4 -->|no| DW6\[No dwelling proxy warning]



&#x20;       DW5 --> DW\_OK\[Dwelling route resolved]

&#x20;       DW6 --> DW\_OK

&#x20;       DW\_OMIT --> DW\_DONE\[Dwelling route complete]

&#x20;       DW\_OK --> DW\_DONE

&#x20;   end



&#x20;   %% =====================================================================

&#x20;   %% AREA-BAND ROUTE

&#x20;   %% =====================================================================

&#x20;   subgraph AREA\[Resolve area-band route]

&#x20;       direction TD

&#x20;       AREA\_START\[Use building\_fire\_damage\_area\\nand building\_total\_damage\_area] --> AR1{Area-band values present\\nwhere required?}



&#x20;       AR1 -->|required value missing / NULL| AR\_OMIT\[Set row outcome = omit\\nAppend missing\_required\_fris\_field warning]

&#x20;       AR1 -->|present| AR2{Area-band values recognised\\nin fire\_event\_mapping\_area\_bands?}



&#x20;       AR2 -->|no| BLOCK\_AREA\[\[Blocking build error\\nArea-band mapping incomplete]]

&#x20;       AR2 -->|yes| AR3\[Resolve fire\_damage\_band\_index\\nResolve total\_damage\_band\_index]



&#x20;       AR3 --> AR4\[n\_tiers = total\_damage\_band\_index - fire\_damage\_band\_index]

&#x20;       AR4 --> AR5{Traffic-light result}



&#x20;       AR5 -->|n\_tiers < 3| AR\_GREEN\[Green\\nAccept total damage band as recorded]

&#x20;       AR5 -->|n\_tiers = 3| AR\_ORANGE\[Orange\\nAccept as recorded\\nAppend suspicious total-damage warning]

&#x20;       AR5 -->|n\_tiers > 3| AR\_RED\[Red\\nImprobable spread in total damage band\\nCap model-facing total damage band\\nat fire damage band + 3 tiers\\nAppend capped total-damage warning]



&#x20;       AR\_GREEN --> AR\_OK\[Area route resolved]

&#x20;       AR\_ORANGE --> AR\_OK

&#x20;       AR\_RED --> AR\_OK

&#x20;       AR\_OMIT --> AR\_DONE\[Area route complete]

&#x20;       AR\_OK --> AR\_DONE

&#x20;   end



&#x20;   %% =====================================================================

&#x20;   %% FIRE SPREAD ROUTE

&#x20;   %% =====================================================================

&#x20;   subgraph FIRE\[Resolve fire spread route]

&#x20;       direction TD

&#x20;       FIRE\_START\[Use heat\_smoke\_damage\_only\\nextent\_of\_damage\\noccupancy\\nfire damage band] --> FS1{heat\_smoke\_damage\_only value}



&#x20;       FS1 -->|yes| FS\_HS\[Set preliminary fire\_spread\_category = heat\_smoke\_only\\nIf fire damage band != None\\nappend inconsistency warning]

&#x20;       FS1 -->|no| FS2{extent\_of\_damage in\\nfire\_event\_mapping\_fire\_cat?}

&#x20;       FS1 -->|NULL| FS\_OMIT\[Set row outcome = omit\\nAppend missing\_required\_fris\_field warning]



&#x20;       FS2 -->|no| BLOCK\_FIRE\[\[Blocking build error\\nFire-category mapping incomplete]]

&#x20;       FS2 -->|yes| FS3\[Resolve preliminary fire\_spread\_category\\nfrom fire\_cat mapping]



&#x20;       FS3 --> FS4{preliminary category\\nrequires occupancy + area-band rule?}

&#x20;       FS4 -->|no| FS5\[Keep mapped fire\_spread\_category]

&#x20;       FS4 -->|yes| FS6{occupancy}



&#x20;       FS6 -->|single| FS\_SINGLE\[Resolve to multiple\_rooms or entire\_dwelling\\nusing single-occupancy extent logic]

&#x20;       FS6 -->|multiple| FS\_MULTI\[Resolve to multiple\_rooms or entire\_dwelling\\nusing multi-occupancy fire damage band rule]

&#x20;       FS6 -->|unknown| FS\_UNKNOWN\[Use configured default or omit\\nAppend occupancy uncertainty warning]



&#x20;       FS\_HS --> FS\_OK\[Fire-spread route resolved]

&#x20;       FS5 --> FS\_OK

&#x20;       FS\_SINGLE --> FS\_OK

&#x20;       FS\_MULTI --> FS\_OK

&#x20;       FS\_UNKNOWN --> FS\_OK

&#x20;       FS\_OMIT --> FS\_DONE\[Fire-spread route complete]

&#x20;       FS\_OK --> FS\_DONE

&#x20;   end



&#x20;   %% =====================================================================

&#x20;   %% ROOM ROUTE

&#x20;   %% =====================================================================

&#x20;   subgraph ROOM\[Resolve room route]

&#x20;       direction TD

&#x20;       ROOM\_START\[Use fire\_start\_location] --> RM1{Does final fire\_spread\_category\\nrequire room\_of\_origin?}



&#x20;       RM1 -->|no| RM\_SKIP\[Do not require room\_of\_origin\\nRetain raw fire\_start\_location for transparency]

&#x20;       RM1 -->|yes| RM2{fire\_start\_location raw value usable?}



&#x20;       RM2 -->|missing / blank / NULL / NaN| RM\_OMIT\_MISSING\[Set row outcome = omit\\nAppend missing\_required\_fris\_field warning]

&#x20;       RM2 -->|present| RM3{fire\_start\_location in\\nfire\_event\_mapping\_rooms?}



&#x20;       RM3 -->|no| BLOCK\_ROOM\[\[Blocking build error\\nRoom mapping incomplete]]

&#x20;       RM3 -->|yes| RM4\[Resolve room\_of\_origin\\nAppend configured room warnings]



&#x20;       RM4 --> RM5{Room mapping says omit\_row?}

&#x20;       RM5 -->|yes| RM\_OMIT\_CONFIG\[Set row outcome = omit\\nAppend configured room omission warning]

&#x20;       RM5 -->|no| RM\_OK\[Room route resolved]



&#x20;       RM\_SKIP --> RM\_OK

&#x20;       RM\_OMIT\_MISSING --> RM\_DONE\[Room route complete]

&#x20;       RM\_OMIT\_CONFIG --> RM\_DONE

&#x20;       RM\_OK --> RM\_DONE

&#x20;   end



&#x20;   %% =====================================================================

&#x20;   %% IGNITION / ITEM ROUTE

&#x20;   %% =====================================================================

&#x20;   subgraph ITEM\[Resolve ignition / item route]

&#x20;       direction TD

&#x20;       ITEM\_START\[Use ignition\_source\_all\\nitem\_first\_ignited\\nfire\_start\_location\\nresolved room\_of\_origin] --> IT1\[Parse ignition\_source\_all\\ninto ignition\_source\_category and ignition\_source]



&#x20;       IT1 --> IT2{Final fire\_spread\_category == single\_item?}

&#x20;       IT2 -->|no| IT\_SKIP\[Retain item fields for transparency\\nDo not omit based on item status,\\nitem properties, or item mapping validity]

&#x20;       IT2 -->|yes| IT3{Usable item/proxy resolved\\nfrom item mapping?}



&#x20;       IT3 -->|yes| IT\_OK\[Resolve item\_combusted\\nand single\_item\_status]

&#x20;       IT3 -->|no| IT4{Single-item fallback available\\nfrom item\_first\_ignited + room?}



&#x20;       IT4 -->|yes| IT\_FALLBACK\[Resolve contextual proxy item\\nAppend item proxy warning]

&#x20;       IT4 -->|no| IT\_OMIT\[Set row outcome = omit\\nAppend unmapped\_single\_item warning]



&#x20;       IT\_SKIP --> IT\_DONE\[Ignition/item route complete]

&#x20;       IT\_OK --> IT\_DONE

&#x20;       IT\_FALLBACK --> IT\_DONE

&#x20;       IT\_OMIT --> IT\_DONE

&#x20;   end



&#x20;   %% =====================================================================

&#x20;   %% RECOMBINE ROUTES AND FINALISE ROW

&#x20;   %% =====================================================================

&#x20;   DW\_DONE --> JOIN{{Combine route outputs}}

&#x20;   AR\_DONE --> JOIN

&#x20;   FS\_DONE --> JOIN

&#x20;   RM\_DONE --> JOIN

&#x20;   IT\_DONE --> JOIN



&#x20;   JOIN --> FINAL1{Any route set row outcome = omit?}

&#x20;   FINAL1 -->|yes| FINAL\_OMIT\[Write omission summary\\nAppend row-level warnings\\nDo not insert usable fire\_events model row]

&#x20;   FINAL1 -->|no| FINAL2\[Create model-facing fire\_events row]



&#x20;   FINAL2 --> FINAL3\[Set data\_quality\_status\\nSet suspicious\_fields\\nSet omit\_from\_model = no\\nAttach accumulated warnings]

&#x20;   FINAL3 --> WRITE\[Insert fire\_events row\\nInsert fire\_event\_warnings rows]



&#x20;   FINAL\_OMIT --> WRITE\_OMIT\[Insert omitted-row record and warnings\\nif schema retains omitted incidents]



&#x20;   WRITE --> MORE{More FRIS rows?}

&#x20;   WRITE\_OMIT --> MORE

&#x20;   OMIT\_REQUIRED --> MORE

&#x20;   OMIT\_HEATSMOKE --> MORE



&#x20;   MORE -->|yes| ROW\_START

&#x20;   MORE -->|no| SUMMARY\[Print build summary\\nrows read / inserted / omitted\\nwarnings / blocking checks passed]

&#x20;   SUMMARY --> END(\[End FRIS build])

```





