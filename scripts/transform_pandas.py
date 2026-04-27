import pandas as pd
import json
import os

def transform_energyzero(raw_file: str):
    # Dosyanın varlığını kontrol et
    if not os.path.exists(raw_file):
        print(f"❌ HATA: Ham veri dosyası bulunamadı: {raw_file}")
        return None

    with open(raw_file) as f:
        data = json.load(f)

    # Prices listesini DataFrame'e çevir
    df = pd.DataFrame(data["Prices"])
    
    # --- ADIM 3: Tarih ve Saat Ayırma ---
    # readingDate sütununu datetime formatına çeviriyoruz
    df["timestamp"] = pd.to_datetime(df["readingDate"])
    
    # Yeni sütunları tam olarak oluşturduğumuzdan emin olalım
    df["Date"] = df["timestamp"].dt.date.astype(str) # Parquet uyumluluğu için string'e çevirmek daha güvenli
    df["Time"] = df["timestamp"].dt.time.astype(str)

    # --- ADIM 4: Vergi Hesaplama (%21) ---
    df["Price_with_VAT"] = df["price"] * 1.21

    # İstediğimiz sütunları seçelim
    final_df = df[["timestamp", "Date", "Time", "price", "Price_with_VAT"]].copy()
    
    # Klasör yolunu otomatik ayarla (Hem Docker'da hem PC'de çalışması için)
    # Eğer '/opt/airflow/' ile başlıyorsa Docker'dadır, değilse yereldedir.
    processed_dir = raw_file.replace("raw", "processed").replace(os.path.basename(raw_file), "")
    os.makedirs(processed_dir, exist_ok=True)
    
    out = raw_file.replace("raw", "processed").replace(".json", ".parquet")
    
    # Parquet olarak kaydet
    final_df.to_parquet(out, index=False, engine='pyarrow')

    print(f"✅ İşlem Başarılı!")
    print(f"📊 Yeni Sütunlar: {final_df.columns.tolist()}")
    print(f"📂 Kaydedilen yer: {out}")
    return out

if __name__ == "__main__":
    # Test için dosya yolunu kontrol et
    test_path = "data/raw/energyzero_20260423.json"
    transform_energyzero(test_path)