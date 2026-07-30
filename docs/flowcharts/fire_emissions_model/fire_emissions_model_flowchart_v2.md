# Fire Emissions deterministic model flow chart

## Purpose

This flow chart summarises the planned deterministic fire-impact/emissions model.

The model takes resolved rows from `fire_events`, inventory snapshot/lookup tables in `fire_db`, and deterministic emission parameters from `fire_emission_parameter_mapping`.

The model has two main stages:

1. Stage 1: affected stock / embodied replacement pathway.
2. Stage 2: direct gas-species emissions pathway.

The first-pass model does not implement stochastic or sensitivity analysis, although it preserves lower/default/upper ranges where already available.

---

## High-level process

```mermaid
flowchart TD

    A[Start model run] --> B[Create model_run_id]
    B --> C[Validate required schema]
    C --> D[Load resolved fire_events]
    D --> E[Filter modelled events]

    E --> F[Load inventory snapshot / lookup tables]
    F --> G[Load fire_emission_parameter_mapping]
    G --> H[Load area-band metadata and resolved area values]

    H --> I[For each fire event]
    I --> J[Normalise fire_spread_category]
    J --> K[Resolve required stock and size inputs]
    K --> K1[Resolve affected_dwelling_count]
    K1 --> L[Stage 1: build affected-stock components]
    L --> M[Stage 2: calculate direct CO2 / CO emissions]
    M --> N[Aggregate component results to event result]
    N --> O[Write component results]
    O --> P[Write event results]
    P --> Q[Write model warnings]
    Q --> R{More events?}
    R -- Yes --> I
    R -- No --> S[Write model run summary]
    S --> T[End]
```

---

## Stage 1 and Stage 2 separation

```mermaid
flowchart LR

    A[Resolved fire event] --> B[Stage 1: affected stock model]

    B --> C[Direct pathway: affected carbon stock kgC]
    B --> D[Replacement pathway: affected embodied CO2]

    C --> E[Stage 2: direct emissions model]
    D --> F[Replacement embodied CO2 output]

    E --> G[Direct CO2 output]
    E --> H[Direct CO output]

    G --> I[Event-level total]
    H --> I
    F --> I
```

Key distinction:

```text
Direct pathway:
    inventory carbon stock kgC
    -> combusted kgC
    -> direct CO2 / CO produced

Replacement pathway:
    inventory room/dwelling embodied CO2
    -> replacement-affected embodied CO2
```

Replacement embodied CO2 is not converted from combusted kgC. It uses the room-level embodied CO2 values now appended to `inventory_room_snapshot`.

---

## Event category routing

```mermaid
flowchart TD

    A[fire_spread_category] --> B{Category}

    B -->|heat_smoke_damage_only or heat_smoke| C[Heat/smoke-only route]
    B -->|single_item| D[Single-item route]
    B -->|within_room| E[Within-room route]
    B -->|multiple_rooms| F[Multiple-rooms route]
    B -->|entire_dwelling| G[Entire-dwelling route]
    B -->|none / roof / unspecified / omitted| H[Omit or zero-output route]

    C --> C1[Direct = 0]
    C --> C2[Replacement = 0 in first-pass model]
    C --> C3[Warn if non-zero damage area ignored]

    D --> D1[Direct = valid item carbon stock]
    D --> D2[Replacement = room embodied CO2 × room damage fraction]
    D --> D3[Invalid single-item cases ignored for now]

    E --> E1[Direct = origin room carbon × room fire fraction]
    E --> E2[Replacement = origin room embodied CO2 × total damage fraction]
    E --> E3[Cap replacement at full origin room]

    F --> F1[Direct = full origin room + residual dwelling fire fraction]
    F --> F2[Replacement = dwelling embodied CO2 × total damage fraction]
    F --> F3[Cap replacement at full dwelling]

    G --> G1[Resolve affected_dwelling_count]
    G1 --> G2[Direct = whole dwelling carbon stock × affected_dwelling_count]
    G1 --> G3[Replacement = whole dwelling embodied CO2 × affected_dwelling_count]
```

---

## Stage 1: affected stock / replacement components

```mermaid
flowchart TD

    A[Event enters Stage 1] --> B[Resolve fire damage area]
    A --> C[Resolve total damage area]
    A --> D[Resolve room_of_origin size]
    A --> E[Resolve dwelling size]
    A --> F[Resolve affected_dwelling_count]

    B --> G[Calculate direct affected fractions]
    C --> H[Calculate replacement affected fractions]

    D --> I[Load origin room stock]
    E --> J[Load dwelling stock]
    I --> K[Load origin room embodied CO2]
    J --> L[Calculate dwelling embodied CO2]

    G --> M[Build direct carbon-stock components]
    H --> N[Build replacement embodied-CO2 components]
    F --> M
    F --> N

    M --> O[Stage 1 component rows]
    N --> O
```

Stage 1 should preserve ranges where available:

```text
direct_total_kgC_lower / default / upper
direct_biogenic_kgC_lower / default / upper
direct_fossil_kgC_lower / default / upper

replacement_embodied_CO2_kg_lower / default / upper
```

Exact suffixes should match the existing inventory snapshot column naming once the code is inspected.

---

## Area handling

```mermaid
flowchart TD

    A[Raw FRIS area band] --> B[Resolver maps band to index]
    B --> C[Resolver compares fire and total damage bands]
    C --> D{Total damage band suspicious?}

    D -- No --> E[Use recorded total damage area band]
    D -- Yes, within warning threshold --> F[Use recorded band and flag suspicious]
    D -- Yes, too large --> G[Cap total damage band to allowed index difference]

    E --> H[Model converts band to lower/default/upper m2]
    F --> H
    G --> H

    H --> I[Use fire damage area for direct pathway]
    H --> J[Use total damage area for replacement pathway]
    H --> K[Use fire damage band index for large multiple-occupancy dwelling count]
```

Planned area-band value rule:

```text
For closed area bands:
    lower   = lower bound
    default = lower bound + 0.33 × band width
    upper   = upper bound

For "None":
    lower = default = upper = 0

For open-ended bands:
    apply controlled caps from dwelling size, room size, or explicit model assumptions.
```

The model should rely on existing resolver traffic-light handling for suspicious fire/total damage-area mismatches, rather than micromanaging every unlikely FRIS entry again.

---

## Multiple-occupancy affected dwelling count

This Stage 1 model assumption corrects a known undercount risk for large fires in multiple-occupancy dwellings. FRIS can record large fires involving multiple dwellings within a block as a single incident. Treating these as one affected dwelling would therefore underestimate both direct affected stock and replacement embodied CO2.

```mermaid
flowchart TD

    A[Fire event] --> B{occupancy = multiple?}
    B -- No --> C[affected_dwelling_count = 1]
    B -- Yes --> D{fire_spread_category = entire_dwelling?}
    D -- No --> C
    D -- Yes --> E{building_fire_damage_area_band_index > 6?}
    E -- No --> C
    E -- Yes --> F[Estimate count from fire damage area upper band]

    F --> G[101-200 m2 -> 2 dwellings]
    F --> H[201-500 m2 -> 5 dwellings]
    F --> I[501-1,000 m2 -> 10 dwellings]
    F --> J[1,001-2,000 m2 -> 20 dwellings]
    F --> K[2,001-5,000 m2 -> 50 dwellings]

    C --> L[Apply count to whole-dwelling direct and replacement components]
    G --> L
    H --> L
    I --> L
    J --> L
    K --> L
```

First-pass lookup:

```text
building_fire_damage_area_band_index = 6  -> 1 dwelling   # 51-100 m2
building_fire_damage_area_band_index = 7  -> 2 dwellings  # 101-200 m2
building_fire_damage_area_band_index = 8  -> 5 dwellings  # 201-500 m2
building_fire_damage_area_band_index = 9  -> 10 dwellings # 501-1,000 m2
building_fire_damage_area_band_index = 10 -> 20 dwellings # 1,001-2,000 m2
building_fire_damage_area_band_index = 11 -> 50 dwellings # 2,001-5,000 m2
```

Resolution:

```text
This rule is only applied when:
    occupancy = 'multiple'
    AND fire_spread_category = 'entire_dwelling'
    AND building_fire_damage_area_band_index > 6

The estimate uses one affected dwelling per 100 m2 of the upper fire-damage area band.
This is intentionally conservative and may underestimate affected dwellings in high-density blocks or tower blocks.

Future work could interpolate more realistic values within area bands and divide by dwelling-type-specific floor area, but that would require additional empirical case research.
```

The affected dwelling count should be carried into Stage 1 output rows where possible, ideally as:

```text
affected_dwelling_count
affected_dwelling_count_method
```

Suggested method values:

```text
single_dwelling_default
multiple_occupancy_fire_damage_area_upper_band_per_100m2
```

---

## Stage 2: direct emissions calculation

```mermaid
flowchart TD

    A[Stage 1 direct carbon component] --> B{Is direct affected kgC > 0?}

    B -- No --> C[CO2 = 0; CO = 0]
    B -- Yes --> D[Load category emission parameters]

    D --> E[Calculate post_flashover_weighting]
    E --> F[Blend overventilated and underventilated species factors]

    F --> G[Calculate combusted kgC]
    G --> H[Apply species carbon fractions]
    H --> I[Apply molecular conversion factors]

    I --> J[CO2 fossil / biogenic / total]
    I --> K[CO fossil / biogenic / total]

    J --> L[Component emissions result]
    K --> L
```

General formula:

```text
emitted_species_kg =
    direct_affected_kgC
    × combustion_completeness_factor
    × char_formation_factor
    × species_emission_factor
    × molecular_conversion_factor
```

Current molecular conversion factors:

```text
C_to_CO2_conversion_factor = 44 / 12
C_to_CO_conversion_factor  = 28 / 12
```

Current species:

```text
CO2
CO
```

The calculation should be generic enough to support additional species later, provided they are added to `fire_emission_parameter_mapping` and a molecular conversion factor exists.

---

## Fire-development / species-factor blending

```mermaid
flowchart TD

    A[Combustion component] --> B{Component route}

    B -->|single_item| C[post_flashover_weighting = 0]
    B -->|within_room fractional| D[Use room_fire_fraction sigmoid]
    B -->|full room / whole dwelling| E[Use complete-combustion integrated sigmoid]
    B -->|residual dwelling fraction| F[Use residual_fire_fraction sigmoid]

    C --> G[Blend species factors]
    D --> G
    E --> G
    F --> G

    G --> H[CO2_emission_factor]
    G --> I[CO_emission_factor]
    H --> J[carbon_partition_sum]
    I --> J
    J --> K{carbon_partition_sum <= 1?}
    K -- Yes --> L[Proceed]
    K -- No --> M[Write strong warning / block depending severity]
```

Species-factor blending:

```text
species_emission_factor =
    (1 - post_flashover_weighting) × species_emission_factor_overventilated
    + post_flashover_weighting × species_emission_factor_underventilated
```

Interpretation:

```text
post_flashover_weighting = 0
    fully overventilated / pre-flashover endpoint

post_flashover_weighting = 1
    fully underventilated / post-flashover endpoint
```

`room_fire_fraction` is a post-event fire-development proxy. It should not be described as a true equivalence ratio.

---

## Category-specific component logic

### heat_smoke_damage_only

```mermaid
flowchart TD

    A[heat_smoke_damage_only event] --> B[Normalise heat_smoke if present]
    B --> C[Direct carbon stock = 0]
    C --> D[Direct CO2 / CO = 0]
    B --> E[Replacement embodied CO2 = 0 in first pass]
    E --> F{Total damage area non-zero?}
    F -- Yes --> G[Write warning: damage area ignored]
    F -- No --> H[No replacement warning needed]
```

Resolution:

```text
Assume zero direct and zero replacement emissions for first-pass model.
This avoids overestimating replacement from suspicious heat/smoke-only FRIS damage-area entries.
```

---

### single_item

```mermaid
flowchart TD

    A[single_item event] --> B{single_item_status}

    B -->|direct_inventory_item| C[Use item_combusted lookup]
    B -->|proxy_inventory_item| C
    B -->|conditionally_inferred_item| C
    B -->|invalid_single_item| D[Ignore direct and replacement pathways for now]

    C --> E[Direct affected carbon = item carbon stock]
    E --> F[post_flashover_weighting = 0]
    F --> G[Calculate CO2 / CO]

    C --> H[Estimate replacement room damage fraction if available]
    H --> I[Replacement embodied CO2 = room embodied CO2 × room damage fraction]
    I --> J[Cap at full origin room embodied CO2]

    D --> K[Write warning / document omitted contribution]
```

Resolution:

```text
Invalid single-item cases are deliberately ignored in the first-pass model due to time constraints.
They therefore do not contribute to direct or embodied/replacement emissions.
```

---

### within_room

```mermaid
flowchart TD

    A[within_room event] --> B[Resolve origin room stock]
    A --> C[Resolve fire damage area]
    A --> D[Resolve total damage area]
    A --> E[Resolve room size]

    B --> F[room_fire_fraction = fire_damage_area / room_size]
    C --> F
    E --> F
    F --> G[Cap room_fire_fraction to 0-1]
    G --> H[Direct affected carbon = room carbon × room_fire_fraction]

    D --> I[room_damage_fraction = total_damage_area / room_size]
    E --> I
    I --> J[Cap room_damage_fraction to 0-1]
    J --> K[Replacement embodied CO2 = room embodied CO2 × room_damage_fraction]

    H --> L[Stage 2 direct CO2 / CO]
    K --> M[Replacement embodied CO2 output]
```

Resolution:

```text
Replacement is capped at full origin-room embodied CO2.
Do not allocate residual dwelling replacement for within-room events, even if total_damage_area appears larger than the room.
Suspicious total damage area should be warned, not micromanaged.
```

---

### multiple_rooms

```mermaid
flowchart TD

    A[multiple_rooms event] --> B[Resolve origin room stock]
    A --> C[Resolve dwelling stock]
    A --> D[Resolve fire damage area]
    A --> E[Resolve total damage area]
    A --> F[Resolve room and dwelling sizes]

    B --> G[Origin-room direct component = full origin room]
    B --> H

    C --> H[Residual dwelling stock = dwelling stock - origin room stock]
    D --> I[residual_fire_area = max fire_area - room_size, 0]
    F --> J[residual_dwelling_area = max dwelling_size - room_size, 0]
    I --> K[residual_fire_fraction]
    J --> K
    K --> L[Cap residual_fire_fraction to 0-1]
    H --> M[Residual direct component = residual stock × residual_fire_fraction]
    L --> M

    E --> N[dwelling_damage_fraction = total_damage_area / dwelling_size]
    F --> N
    N --> O[Cap dwelling_damage_fraction to 0-1]
    O --> P[Replacement embodied CO2 = dwelling embodied CO2 × dwelling_damage_fraction]

    G --> Q[Stage 2 emissions for origin-room component]
    M --> R[Stage 2 emissions for residual component]
    P --> S[Replacement embodied CO2 output]
```

Resolution:

```text
Direct pathway is component-based:
    full origin room + residual dwelling fire fraction.

Replacement pathway uses total damage fraction over the dwelling and is capped at full dwelling embodied CO2.
```

---

### entire_dwelling

```mermaid
flowchart TD

    A[entire_dwelling event] --> B[Load whole dwelling carbon stock]
    A --> C[Load whole dwelling embodied CO2]
    A --> D[Resolve affected_dwelling_count]

    D --> E{Large multiple-occupancy fire?}
    E -- No --> F[affected_dwelling_count = 1]
    E -- Yes --> G[Estimate from fire damage area band]

    B --> H[Direct affected carbon = whole dwelling carbon × affected_dwelling_count]
    C --> I[Replacement embodied CO2 = whole dwelling embodied CO2 × affected_dwelling_count]
    F --> H
    F --> I
    G --> H
    G --> I

    H --> J[Use complete-combustion transition]
    J --> K[Calculate direct CO2 / CO]

    I --> L[Replacement embodied CO2 output]
```

Resolution:

```text
For ordinary entire-dwelling events, affected_dwelling_count = 1.

For multiple-occupancy entire-dwelling events with building_fire_damage_area_band_index > 6, estimate affected_dwelling_count from the upper fire-damage area band using one dwelling per 100 m2.

Apply affected_dwelling_count to both:
    direct whole-dwelling carbon stock
    replacement whole-dwelling embodied CO2

Fire and total damage areas can still be retained as diagnostics.
```

---

## Results writing

```mermaid
flowchart TD

    A[Component calculations complete] --> B[Write fire_model_component_results]
    B --> C[Aggregate by source_id / incident_id / estimate_case]
    C --> D[Write fire_model_event_results]
    D --> E[Write fire_model_warnings]
    E --> F[Write fire_model_runs summary]
```

Suggested result tables:

```text
fire_model_runs
fire_model_component_results
fire_model_event_results
fire_model_warnings
```

Suggested run metadata:

```text
model_run_id
model_version
created_at_utc
parameter_source_id
inventory_snapshot_id
input_type
estimate_case
run_notes
```

Suggested component types:

```text
heat_smoke_damage_only
single_item
origin_room
within_room_fraction
residual_dwelling
whole_dwelling
```

Suggested estimate cases:

```text
lower
default
upper
```

The first implemented result can use only `default`, with `lower` and `upper` added once the central model is stable.

---

## Model warnings and limitations

```mermaid
flowchart TD

    A[During event/component calculation] --> B{Issue found?}
    B -- No --> C[Continue]
    B -- Yes --> D[Create structured warning]
    D --> E[Attach to model_run_id and source_id]
    E --> F[Write fire_model_warnings]
```

Warnings to support:

```text
HEAT_SMOKE_DAMAGE_AREA_IGNORED
HEAT_SMOKE_REPLACEMENT_ASSUMED_ZERO
INVALID_SINGLE_ITEM_IGNORED
ROOM_SIZE_MEAN_USED
DWELLING_SIZE_MEAN_USED
FIRE_AREA_CAPPED_TO_ROOM_SIZE
FIRE_AREA_CAPPED_TO_DWELLING_SIZE
TOTAL_DAMAGE_AREA_CAPPED_TO_ROOM_SIZE
TOTAL_DAMAGE_AREA_CAPPED_TO_DWELLING_SIZE
MULTIPLE_OCCUPANCY_AFFECTED_DWELLING_COUNT_ESTIMATED
MULTIPLE_OCCUPANCY_AFFECTED_DWELLING_COUNT_OPEN_ENDED
RESIDUAL_DWELLING_AREA_ZERO
RESIDUAL_STOCK_FALLBACK_USED
CARBON_PARTITION_EXCEEDS_ONE
CHAR_FORMATION_PARAMETER_NEUTRAL
OMITTED_EVENT_NO_MODEL_CONTRIBUTION
```

Important documented limitations:

```text
1. heat_smoke_damage_only events are assigned zero replacement emissions in the first-pass model.
2. invalid_single_item cases are ignored and therefore do not contribute to direct or embodied emissions.
3. omitted fire_events rows cannot contribute to the model outputs.
4. suspicious FRIS area-band inputs are handled primarily by resolver warnings and capped model values, not manual case-by-case corrections.
5. direct fire-produced CO2 / CO is kept separate from replacement embodied CO2.
6. replacement embodied CO2 is based on room/dwelling embodied CO2 values in inventory_room_snapshot.
7. large multiple-occupancy entire-dwelling fires estimate affected_dwelling_count from broad FRIS fire-damage area bands using one dwelling per 100 m2 of the upper band. This is a conservative first-pass assumption and may underestimate high-density block or tower-block fires.
```
