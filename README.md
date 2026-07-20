# Fire Emissions

Research code for estimating emissions from anthropogenic fires.

Instructions:
Create a virtual environment, then install dependencies with "pip install -r requirements.txt"


Recommended workflow:
0. Create your local copy of `local_paths.yaml`, using `local_paths.axample.yaml` as a template/guide.
1. Initialise the blank Inventory Database:
`python -m scripts.inventory.init_inventory_db --profile <profile> --db inventory_db`
2. Ingest the inventory vocabulary file:
Dry run: `python -m scripts.ingest --profile <profile> --db inventory_db --type vocab --scan`
Write:   `python -m scripts.ingest --profile <profile> --db inventory_db --type vocab --scan --apply`
3. Ingest the inventory survey raw data file:
Dry run: `python -m scripts.ingest --profile <profile> --db test_db --type survey --scan`
Write:   `python -m scripts.ingest --profile <profile> --db test_db --type survey --scan --apply` 
4. Ingest the assumed inventory file:
Dry run: `python -m scripts.ingest --profile <profile> --db inventory_db --type assumed --scan`
Write:   `python -m scripts.ingest --profile <profile> --db inventory_db --type assumed --scan --apply`
5. Run the Amazon price scraper:
`python -m scripts.lca.fetch_amazon_prices --profile tom --db inventory_db`
6. Build the carbon inventory distribution PMF tables:
`python -m scripts.model --profile <profile> --db inventory_db --type inventory`
7. Build the room type-dependent carbon summary tables:
`python -m scripts.model --profile <profile> --db inventory_db --type room_carbon`
8. Intialise the blank Fire Database:
`python -m scripts.fire.init_fire_db --profile <profile> --db fire_db`
9. Build the Inventory Database snapshots in the Fire Database:
`python -m scripts.fire.inventory_snapshot --profile <profile> --source-db inventory_db --destination-db fire_db --apply`
10. Ingest a single fire incident:
Dry run: `python -m scripts.ingest --profile <profile> --db test_db --type single --scan`
Write: `python -m scripts.ingest --profile <profile> --db test_db --type single --scan --apply`
11. Ingest the FRIS bulk fire incident data:
Dry run: `python -m scripts.ingest --profile <profile> --db test_db --type fris --scan`
Write: `python -m scripts.ingest --profile <profile> --db test_db --type fris --scan --apply`
12. Ingest the fire event mapping tables:
Dry run: `python -m scripts.ingest --profile <profile> --db fire_db --type fire_mappings --scan`
Write: `python -m scripts.ingest --profile <profile> --db fire_db --type fire_mappings --scan --apply`
13.
14. Ingest the emission factor model parameters:
Dry run: `python -m scripts.ingest --profile <profile> --db fire_db --type emissions --scan`
Write: `python -m scripts.ingest --profile <profile> --db fire_db --type emissions --scan --apply`

python -m scripts.inventory.init_inventory_db --profile tom --db inventory_db
python -m scripts.ingest --profile tom --db inventory_db --type vocab --scan --apply
python -m scripts.ingest --profile tom --db inventory_db --type survey --scan --apply
python -m scripts.ingest --profile tom --db inventory_db --type assumed --scan --apply
python -m scripts.model --profile tom --db inventory_db --type inventory
python -m scripts.model --profile tom --db inventory_db --type room_carbon
python -m scripts.lca.fetch_amazon_prices --profile tom --db inventory_db

python -m scripts.fire.init_fire_db --profile tom --db fire_db
python -m scripts.fire.inventory_snapshot --profile tom --source-db inventory_db --destination-db fire_db --apply
python -m scripts.ingest --profile tom --db fire_db --type fris --scan --apply
python -m scripts.ingest --profile tom --db fire_db --type fire_mappings --scan --apply
python -m scripts.fire.build_fire_events --profile tom --db fire_db --type fris --apply
python -m scripts.ingest --profile tom --db fire_db --type emissions --scan --apply


Additional documents:
- `docs/ingest.md` for info on ingestion
- `docs/cli.md` for ingest-based command line inputs
- `docs/db_structure.md` for database schema
- `docs/model.md` for info on running the model



.\.venv\Scripts\Activate.ps1