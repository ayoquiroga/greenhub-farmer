import duckdb, os

raw = r'C:\Users\aYo\Documents\DataScientist\datatalkclub\data-engineering-zoomcamp\data-engineering-zoomcamp-2026\GreenHubFarmer\data\raw'

print('=== ESTRUCTURA DE CARPETAS ===')
for entry in sorted(os.scandir(raw), key=lambda e: e.name):
    if entry.is_dir():
        files = [f for f in os.scandir(entry.path) if f.name.endswith('.parquet')]
        print(f'  {entry.name}/  ({len(files)} archivos parquet)')
    else:
        print(f'  {entry.name}')

print()

for folder in sorted(os.listdir(raw)):
    folder_path = os.path.join(raw, folder)
    if not os.path.isdir(folder_path):
        continue
    parts = sorted([f.path for f in os.scandir(folder_path) if f.name.endswith('.parquet')])
    if not parts:
        continue
    print(f'=== SCHEMA: {folder} ===')
    cols = duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{parts[0]}')").fetchall()
    for col in cols:
        print(f'  {col[0]:<45} {col[1]}')
    print(f'  → {len(cols)} columnas | primer archivo: {os.path.basename(parts[0])}')
    glob_path = folder_path.replace('\\', '/') + '/*.parquet'
    cnt = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{glob_path}')").fetchone()[0]
    print(f'  → {cnt:,} filas totales en la carpeta')
    print()
