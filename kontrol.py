import pandas as pd
import os

# 1. Dosyanın ismini tam olarak klasöründe gördüğün gibi yaz
dosya_adi = 'energyzero_20260423.parquet'

# 2. Farklı klasör derinliklerini dene (Dags içindeysen bir üst klasöre bakması gerekebilir)
yollar = [
    f'data/processed/{dosya_adi}',
    f'../data/processed/{dosya_adi}',
    f'./data/processed/{dosya_adi}'
]

bulundu = False
for yol in yollar:
    if os.path.exists(yol):
        print(f"✅ Dosya bulundu: {yol}")
        try:
            df = pd.read_parquet(yol)
            print("\n--- SÜTUNLAR ---")
            print(df.columns.tolist())
            
            print("\n--- İLK 5 SATIR ---")
            # Eğer Price_with_VAT yoksa hata vermemesi için güvenli yazdırıyoruz
            columns_to_show = [c for c in ['Date', 'Time', 'price', 'Price_with_VAT'] if c in df.columns]
            print(df[columns_to_show].head())
            bulundu = True
            break
        except Exception as e:
            print(f"❌ Dosya okuma hatası: {e}")

if not bulundu:
    print("❌ HATA: Parquet dosyası hiçbir yolda bulunamadı!")
    print(f"Şu anki konumun: {os.getcwd()}")
    print("Klasördeki dosyalar:", os.listdir('.'))