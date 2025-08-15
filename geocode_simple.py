#!/usr/bin/env python3
import csv
import json
import time
import urllib.request
import urllib.parse
from typing import Dict, List, Optional, Tuple

def geocode_address_nominatim(address: str) -> Optional[Tuple[float, float]]:
    """
    Nominatim (OpenStreetMap) を使用して住所をジオコーディング
    """
    url = "https://nominatim.openstreetmap.org/search?"
    params = {
        "q": address,
        "format": "json",
        "limit": "1",
        "countrycodes": "jp"
    }
    
    full_url = url + urllib.parse.urlencode(params)
    
    try:
        req = urllib.request.Request(full_url)
        req.add_header("User-Agent", "EvacuationSiteMapper/1.0")
        
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            
            if data and len(data) > 0:
                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])
                return lat, lon
    except Exception as e:
        print(f"Nominatim APIエラー ({address}): {e}")
    
    return None

def geocode_address_gsi(address: str) -> Optional[Tuple[float, float]]:
    """
    国土地理院のジオコーディングAPIを使用
    """
    url = "https://msearch.gsi.go.jp/address-search/AddressSearch?"
    params = {"q": address}
    
    full_url = url + urllib.parse.urlencode(params)
    
    try:
        with urllib.request.urlopen(full_url, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            
            if data and len(data) > 0:
                geometry = data[0].get("geometry", {})
                coordinates = geometry.get("coordinates", [])
                if len(coordinates) >= 2:
                    lon, lat = coordinates[0], coordinates[1]
                    return lat, lon
    except Exception as e:
        print(f"国土地理院APIエラー ({address}): {e}")
    
    return None

def geocode_with_fallback(address: str) -> Optional[Tuple[float, float]]:
    """
    複数のジオコーディングサービスを試行
    """
    print(f"ジオコーディング中: {address}")
    
    # まず国土地理院APIを試す
    coords = geocode_address_gsi(address)
    if coords:
        print(f"  → 成功 (国土地理院): {coords[0]:.6f}, {coords[1]:.6f}")
        return coords
    
    # フォールバックとしてNominatimを使用
    time.sleep(1)  # レート制限対策
    coords = geocode_address_nominatim(address)
    if coords:
        print(f"  → 成功 (Nominatim): {coords[0]:.6f}, {coords[1]:.6f}")
        return coords
    
    print(f"  → 失敗: 座標を取得できませんでした")
    return None

def process_evacuation_sites(input_csv: str, output_json: str):
    """
    CSVファイルから震災救援所データを読み込み、ジオコーディングしてJSONに保存
    """
    sites = []
    failed_sites = []
    
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, 1):
            name = row['施設名']
            address = row['所在地']
            
            print(f"\n[{idx}/65] 処理中...")
            
            # ジオコーディング実行
            coords = geocode_with_fallback(address)
            
            if coords:
                site = {
                    "name": name,
                    "address": address,
                    "latitude": coords[0],
                    "longitude": coords[1]
                }
                sites.append(site)
            else:
                failed_sites.append({
                    "name": name,
                    "address": address
                })
            
            # API制限対策で少し待機
            time.sleep(0.5)
    
    # 結果をJSONファイルに保存
    result = {
        "type": "FeatureCollection",
        "features": [],
        "metadata": {
            "title": "杉並区震災救援所",
            "total_sites": len(sites),
            "failed_geocoding": len(failed_sites),
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    }
    
    # GeoJSON形式のフィーチャーを作成
    for site in sites:
        feature = {
            "type": "Feature",
            "properties": {
                "name": site["name"],
                "address": site["address"]
            },
            "geometry": {
                "type": "Point",
                "coordinates": [site["longitude"], site["latitude"]]
            }
        }
        result["features"].append(feature)
    
    # 失敗したサイトも記録
    if failed_sites:
        result["failed_sites"] = failed_sites
    
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"\n" + "=" * 60)
    print(f"処理完了:")
    print(f"  成功: {len(sites)} 件")
    print(f"  失敗: {len(failed_sites)} 件")
    print(f"  出力ファイル: {output_json}")
    
    if failed_sites:
        print("\nジオコーディングに失敗した施設:")
        for site in failed_sites:
            print(f"  - {site['name']}: {site['address']}")

if __name__ == "__main__":
    input_csv = "evacuation_sites.csv"
    output_json = "evacuation_sites_geocoded.json"
    
    print("杉並区震災救援所のジオコーディングを開始します...")
    print("=" * 60)
    
    process_evacuation_sites(input_csv, output_json)