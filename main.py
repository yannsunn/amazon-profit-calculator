#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import tempfile
import logging
import csv
import io
import json
import shutil
import math
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static', static_url_path='/static')
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'amazon-profit-calculator-2024')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB

CORS(app)

ALLOWED_EXTENSIONS = {'csv'}

DATA_DIR = '/tmp/monthly_data' if os.environ.get('VERCEL') else 'monthly_data'

def ensure_data_dir():
    """Ensure data directory exists (for serverless compatibility)"""
    try:
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Failed to create data directory {DATA_DIR}: {e}")
        return False

def generate_month_list():
    months = []
    start_date = datetime(2025, 7, 1)
    end_date = datetime(2026, 7, 1)
    
    current = start_date
    while current <= end_date:
        months.append({
            'key': current.strftime('%Y-%m'),
            'display': f"{current.year}年{current.month}月",
            'year': current.year,
            'month': current.month
        })
        # 次の月へ
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return months

MONTH_LIST = generate_month_list()

def allowed_file(filename):
    """許可されたファイル形式かチェック"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def save_monthly_data(month_key, uploaded_files, results, spreadsheet_data):
    """月別データを保存"""
    try:
        if not ensure_data_dir():
            return False
            
        month_dir = os.path.join(DATA_DIR, month_key)
        if not os.path.exists(month_dir):
            os.makedirs(month_dir)
        
        # メタデータを保存
        metadata = {
            'timestamp': datetime.now().isoformat(),
            'uploaded_files': uploaded_files,
            'month': month_key
        }
        
        with open(os.path.join(month_dir, 'metadata.json'), 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        # 計算結果を保存
        with open(os.path.join(month_dir, 'results.json'), 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        # スプレッドシート形式データを保存
        with open(os.path.join(month_dir, 'spreadsheet.json'), 'w', encoding='utf-8') as f:
            json.dump(spreadsheet_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"月別データ保存完了: {month_key}")
        return True
    except Exception as e:
        logger.error(f"月別データ保存エラー {month_key}: {e}")
        return False

def save_uploaded_files(month_key, file_paths):
    """アップロードされたファイルを月別ディレクトリに保存"""
    try:
        if not ensure_data_dir():
            return {}
            
        month_dir = os.path.join(DATA_DIR, month_key, 'files')
        if not os.path.exists(month_dir):
            os.makedirs(month_dir)
        
        saved_files = {}
        for key, temp_path in file_paths.items():
            if os.path.exists(temp_path):
                dest_path = os.path.join(month_dir, f"{key}.csv")
                shutil.copy2(temp_path, dest_path)
                saved_files[key] = dest_path
        
        return saved_files
    except Exception as e:
        logger.error(f"ファイル保存エラー {month_key}: {e}")
        return {}

def load_monthly_data(month_key):
    """月別データを読み込み"""
    try:
        month_dir = os.path.join(DATA_DIR, month_key)
        if not os.path.exists(month_dir):
            return None
        
        # メタデータを読み込み
        with open(os.path.join(month_dir, 'metadata.json'), 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        # 計算結果を読み込み
        with open(os.path.join(month_dir, 'results.json'), 'r', encoding='utf-8') as f:
            results = json.load(f)
        
        # スプレッドシート形式データを読み込み
        with open(os.path.join(month_dir, 'spreadsheet.json'), 'r', encoding='utf-8') as f:
            spreadsheet_data = json.load(f)
        
        return {
            'metadata': metadata,
            'results': results,
            'spreadsheet_data': spreadsheet_data
        }
    except Exception as e:
        logger.error(f"月別データ読み込みエラー {month_key}: {e}")
        return None

def get_saved_months():
    """保存済み月データのリストを取得"""
    try:
        saved_months = []
        if not ensure_data_dir():
            return []
        if os.path.exists(DATA_DIR):
            for month_key in os.listdir(DATA_DIR):
                month_path = os.path.join(DATA_DIR, month_key)
                if os.path.isdir(month_path) and os.path.exists(os.path.join(month_path, 'metadata.json')):
                    # 月情報を検索
                    month_info = next((m for m in MONTH_LIST if m['key'] == month_key), None)
                    if month_info:
                        saved_months.append({
                            'key': month_key,
                            'display': month_info['display'],
                            'has_data': True
                        })
        
        return sorted(saved_months, key=lambda x: x['key'])
    except Exception as e:
        logger.error(f"保存済み月データ取得エラー: {e}")
        return []

def detect_encoding(file_path):
    """エンコーディング検出（簡易版）"""
    encodings = ['utf-8', 'shift_jis', 'cp932', 'euc-jp']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read()
            return encoding
        except UnicodeDecodeError:
            continue
    
    return 'utf-8'  # デフォルト

def safe_read_csv(file_path, max_rows=50000):
    """安全なCSV読み込み（エラー対策強化版）"""
    try:
        # ファイル存在確認
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"ファイルが見つかりません: {file_path}")
        
        # ファイルサイズチェック（50MB制限）
        file_size = os.path.getsize(file_path)
        if file_size > 50 * 1024 * 1024:
            raise ValueError(f"ファイルサイズが大きすぎます: {file_size/1024/1024:.1f}MB")
        
        encoding = detect_encoding(file_path)
        
        data = []
        with open(file_path, 'r', encoding=encoding, errors='replace') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= max_rows:
                    logger.warning(f"最大行数 {max_rows} に達しました")
                    break
                # 空行をスキップ
                if row and any(v for v in row.values() if v):
                    data.append(row)
        
        if not data:
            logger.warning(f"データが空です: {file_path}")
            return []
        
        logger.info(f"ファイル読み込み成功: {file_path} (エンコーディング: {encoding}, 行数: {len(data)})")
        return data
    except Exception as e:
        logger.error(f"ファイル読み込みエラー: {file_path} - {e}")
        raise

def safe_float_convert(value):
    """安全な数値変換"""
    try:
        if value is None or value == '' or str(value).strip() == '':
            return 0
        # 文字列のクリーニング
        cleaned = str(value).replace(',', '').replace('¥', '').replace('円', '').replace('￥', '').strip()
        if cleaned.lower() in ['none', 'null', 'nan', '-', '']:
            return 0
        return float(cleaned)
    except (ValueError, TypeError, AttributeError):
        return 0

def extract_month_from_date(date_str):
    """日付文字列から年月を抽出（エラー対策強化版）"""
    try:
        if not date_str or str(date_str).strip() == '':
            return "2025-06"
        
        date_str = str(date_str).strip()
        
        # 様々な日付形式に対応
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) >= 2:
                try:
                    # YYYY/MM形式
                    if len(parts[0]) == 4:
                        year = int(parts[0])
                        month = int(parts[1])
                    # MM/DD/YYYY形式
                    elif len(parts) == 3 and len(parts[2]) == 4:
                        month = int(parts[0])
                        year = int(parts[2])
                    # YY/MM形式
                    else:
                        year = int(f"20{parts[0]}")
                        month = int(parts[1])
                    
                    # 妥当性チェック
                    if 2020 <= year <= 2030 and 1 <= month <= 12:
                        return f"{year}-{month:02d}"
                except (ValueError, IndexError):
                    pass
        elif '-' in date_str:
            parts = date_str.split('-')
            if len(parts) >= 2:
                try:
                    year = int(parts[0])
                    month = int(parts[1])
                    if 2020 <= year <= 2030 and 1 <= month <= 12:
                        return f"{year}-{month:02d}"
                except (ValueError, IndexError):
                    pass
        
        # デフォルト
        return "2025-06"
    except Exception as e:
        logger.warning(f"日付解析エラー: {date_str} - {e}")
        return "2025-06"

def process_makad_data(data, account_type='a_m'):
    """マカドデータ処理（スプレッドシート対応版）"""
    try:
        results = {}
        
        for row in data:
            # 日付列を探す
            date_value = None
            for key, value in row.items():
                if '日' in key or 'date' in key.lower():
                    date_value = value
                    break
            
            if not date_value:
                continue
            
            month = extract_month_from_date(str(date_value))
            
            if month not in results:
                results[month] = {
                    'Amazon' if account_type == 'a_m' else 'Amazon2': 0,
                    'メルカリShops': 0,
                    
                    'プラットフォーム手数料_Amazon' if account_type == 'a_m' else 'プラットフォーム手数料_Amazon2': 0,
                    'プラットフォーム手数料_メルカリ': 0,
                    '運送費（送料）': 0,
                    
                    '売上総利益': 0,
                    '売上高合計': 0
                }
            
            amazon_key = 'Amazon' if account_type == 'a_m' else 'Amazon2'
            fee_key = 'プラットフォーム手数料_Amazon' if account_type == 'a_m' else 'プラットフォーム手数料_Amazon2'
            
            for key, value in row.items():
                try:
                    num_value = safe_float_convert(value)
                    
                    if '販売価格' in key:
                        results[month][amazon_key] += num_value
                        results[month]['売上高合計'] += num_value
                    elif '送料' in key:
                        results[month][amazon_key] += num_value
                        results[month]['運送費（送料）'] += num_value
                        results[month]['売上高合計'] += num_value
                    elif 'ポイント' in key or '割引' in key:
                        # ポイントと割引は売上から差し引く
                        results[month][amazon_key] -= num_value
                        results[month]['売上高合計'] -= num_value
                    elif '手数料' in key and 'Amazon' in key:
                        results[month][fee_key] += num_value
                    elif '利益' in key or '粗利' in key:
                        results[month]['売上総利益'] += num_value
                except:
                    continue
        
        for month in results:
            for key in results[month]:
                results[month][key] = int(results[month][key])
        
        return results
    except Exception as e:
        logger.error(f"マカドデータ処理エラー: {e}")
        return {}

def process_mercari_data(data):
    """メルカリショップデータ処理（スプレッドシート対応版）"""
    try:
        results = {}
        
        for row in data:
            # 日付列を探す
            date_value = None
            for key, value in row.items():
                if '日' in key or 'date' in key.lower():
                    date_value = value
                    break
            
            if not date_value:
                continue
            
            month = extract_month_from_date(str(date_value))
            
            if month not in results:
                results[month] = {
                    'Amazon': 0,
                    'Amazon2': 0,
                    'メルカリShops': 0,
                    
                    # 経費セクション
                    'プラットフォーム手数料_Amazon': 0,
                    'プラットフォーム手数料_Amazon2': 0,
                    'プラットフォーム手数料_メルカリ': 0,
                    '運送費（送料）': 0,
                    
                    '売上総利益': 0,
                    '売上高合計': 0
                }
            
            for key, value in row.items():
                try:
                    num_value = safe_float_convert(value)
                    
                    if '売上' in key and '税込' in key:
                        results[month]['メルカリShops'] += num_value
                        results[month]['売上高合計'] += num_value
                    elif '販売手数料' in key and '税込' in key:
                        results[month]['プラットフォーム手数料_メルカリ'] += num_value
                    elif '販売利益' in key or '利益' in key:
                        results[month]['売上総利益'] += num_value
                    elif '送料' in key:
                        results[month]['運送費（送料）'] += num_value
                except:
                    continue
        
        for month in results:
            for key in results[month]:
                results[month][key] = int(results[month][key])
        
        return results
    except Exception as e:
        logger.error(f"メルカリショップデータ処理エラー: {e}")
        return {}

def process_hanro_data(data, account_type='a_m'):
    """販路プラスデータ処理（スプレッドシート対応版）"""
    try:
        results = {}
        
        for row in data:
            # 日付列を探す
            date_value = None
            for key, value in row.items():
                if 'At' in key or '日' in key:
                    date_value = value
                    break
            
            if not date_value:
                continue
            
            month = extract_month_from_date(str(date_value))
            
            if month not in results:
                results[month] = {
                    'Amazon': 0,
                    'Amazon2': 0,
                    'メルカリShops': 0,
                    
                    # 経費セクション
                    'プラットフォーム手数料_Amazon': 0,
                    'プラットフォーム手数料_Amazon2': 0,
                    'プラットフォーム手数料_メルカリ': 0,
                    '運送費（送料）': 0,
                    
                    '売上総利益': 0,
                    '売上高合計': 0
                }
            
            # 販路情報
            mall = row.get('mall', '').lower()
            
            for key, value in row.items():
                try:
                    num_value = safe_float_convert(value)
                    
                    if 'netPrice' in key or '価格' in key or '売上' in key:
                        if mall == 'mercari':
                            results[month]['メルカリShops'] += num_value
                        else:
                            # Default to Amazon account based on account_type
                            amazon_key = 'Amazon' if account_type == 'a_m' else 'Amazon2'
                            results[month][amazon_key] += num_value
                        results[month]['売上高合計'] += num_value
                    elif 'profit' in key or '利益' in key:
                        results[month]['売上総利益'] += num_value
                    elif '送料' in key or 'shipping' in key.lower():
                        results[month]['運送費（送料）'] += num_value
                except:
                    continue
        
        for month in results:
            for key in results[month]:
                results[month][key] = int(results[month][key])
        
        return results
    except Exception as e:
        logger.error(f"販路プラスデータ処理エラー: {e}")
        return {}

# 改善された処理関数をインポート
from process_expense_ad import process_expense_data_improved, process_ad_data_improved

def process_expense_data(data, account_type='a_m'):
    """経費データ処理（Amazonトランザクションレポート対応）"""
    try:
        results = {}
        
        # 最初の数行でカラム名をログ出力
        if data and len(data) > 0:
            all_columns = list(data[0].keys())
            logger.info(f"経費データ全カラム ({account_type}): {all_columns}")
        
        for row_index, row in enumerate(data):
            try:
                # 日付列を探す（BOM付きカラム名にも対応）
                date_value = None
                for key, value in row.items():
                    # BOMを除去してチェック
                    clean_key = key.replace('﻿', '').strip()
                    if '日付' in clean_key or '時間' in clean_key:
                        date_value = value
                        break
                
                if not date_value:
                    continue
            
                month = extract_month_from_date(str(date_value))
                
                if month not in results:
                    account_suffix = 'A-M' if account_type == 'a_m' else 'O-AA'
                    results[month] = {
                        f'Amazon手数料_{account_suffix}': 0,
                        f'FBA手数料_{account_suffix}': 0,
                        f'配送料_{account_suffix}': 0,
                        f'ポイント費用_{account_suffix}': 0,
                        f'その他経費_{account_suffix}': 0,
                        f'経費合計_{account_suffix}': 0
                    }
                
                # 経費データを詳細に抽出（Amazonトランザクションレポートの実際の構造に基づく）
                account_suffix = 'A-M' if account_type == 'a_m' else 'O-AA'
                
                # トランザクションタイプを確認
                transaction_type = row.get('トランザクションの種類', '').strip()
                
                # 合計金額カラムから手数料を取得（「合計」カラムを使用）
                total_amount = 0
                for key in row.keys():
                    clean_key = key.replace('﻿', '').strip()
                    if clean_key == '合計':
                        total_amount = safe_float_convert(row[key])
                        break
                
                # トランザクションタイプに基づいて分類
                if total_amount < 0:  # 負の値は手数料・費用
                    abs_amount = abs(total_amount)
                    
                    # トランザクションタイプまたは説明から分類
                    description = row.get('商品の説明', '').strip() + ' ' + transaction_type
                    
                    # FBA関連手数料
                    if any(term in description for term in ['FBA', 'フルフィルメント', '在庫保管', '出荷作業', '配送代行']):
                        results[month][f'FBA手数料_{account_suffix}'] += abs_amount
                        results[month][f'経費合計_{account_suffix}'] += abs_amount
                    # 販売手数料
                    elif any(term in description for term in ['成約料', 'リファーラル', '販売手数料', 'Commission', '基本成約', 'カテゴリー成約']):
                        results[month][f'Amazon手数料_{account_suffix}'] += abs_amount
                        results[month][f'経費合計_{account_suffix}'] += abs_amount
                    # 配送料
                    elif any(term in description for term in ['配送', 'Shipping', '送料']):
                        results[month][f'配送料_{account_suffix}'] += abs_amount
                        results[month][f'経費合計_{account_suffix}'] += abs_amount
                    # ポイント
                    elif any(term in description for term in ['ポイント', 'Points']):
                        results[month][f'ポイント費用_{account_suffix}'] += abs_amount
                        results[month][f'経費合計_{account_suffix}'] += abs_amount
                    # その他の手数料
                    else:
                        results[month][f'その他経費_{account_suffix}'] += abs_amount
                        results[month][f'経費合計_{account_suffix}'] += abs_amount
            except Exception as e:
                logger.warning(f"経費データ行{row_index}処理エラー: {e}")
                continue
        
        # 整数に変換
        for month in results:
            for key in results[month]:
                results[month][key] = int(results[month][key])
        
        return results
    except Exception as e:
        logger.error(f"経費データ処理エラー: {e}")
        return {}

def process_ad_data(data, account_type='a_m'):
    """広告費データ処理（Amazon広告レポート対応）"""
    try:
        results = {}
        
        # 最初の数行でカラム名をログ出力
        if data and len(data) > 0:
            all_columns = list(data[0].keys())
            logger.info(f"広告費データ全カラム ({account_type}): {all_columns}")
            
            # デバッグ用：支出関連のカラムを特定
            spend_columns = [col for col in all_columns if '支出' in col or 'Spend' in col.lower() or 'Cost' in col.lower()]
            if spend_columns:
                logger.info(f"支出関連カラム: {spend_columns}")
        
        for row_index, row in enumerate(data):
            try:
                # 日付列を探す（複数の可能性をチェック）
                date_value = None
                date_columns = ['開始日', '終了日', '日付', 'Date', 'Start Date', 'End Date']
                
                for col_name in date_columns:
                    for key in row.keys():
                        clean_key = key.replace('\ufeff', '').replace('﻿', '').strip()
                        if col_name in clean_key:
                            date_value = row[key]
                            break
                    if date_value:
                        break
                
                if not date_value:
                    # 日付列が見つからない場合、最初の日付形式の値を探す
                    for value in row.values():
                        if value and ('/' in str(value) or '-' in str(value)):
                            try:
                                test_month = extract_month_from_date(str(value))
                                if test_month != "2025-06":  # デフォルト値でない場合
                                    date_value = value
                                    break
                            except:
                                continue
                
                if not date_value:
                    continue
                
                month = extract_month_from_date(str(date_value))
                
                if month not in results:
                    account_suffix = 'A-M' if account_type == 'a_m' else 'O-AA'
                    results[month] = {
                        f'スポンサープロダクト広告_{account_suffix}': 0,
                        f'広告費合計_{account_suffix}': 0
                    }
                
                account_suffix = 'A-M' if account_type == 'a_m' else 'O-AA'
                
                # 支出カラムから広告費を取得（複数の可能性をチェック）
                spend_value = None
                spend_columns = ['支出', 'Spend', '費用', 'Cost', '広告費', 'Ad Spend']
                
                for col_name in spend_columns:
                    for key in row.keys():
                        clean_key = key.replace('\ufeff', '').replace('﻿', '').strip()
                        if col_name in clean_key:
                            test_value = safe_float_convert(row[key])
                            if test_value > 0:  # 正の値のみ
                                spend_value = test_value
                                break
                    if spend_value:
                        break
                
                # 値が見つからない場合、すべての数値カラムをチェック
                if spend_value is None:
                    for key, value in row.items():
                        if value and value != '':
                            test_value = safe_float_convert(value)
                            # 妥当な広告費の範囲内の値を探す（1円〜1000万円）
                            if 1 <= test_value <= 10000000:
                                clean_key = key.replace('\ufeff', '').replace('﻿', '').strip()
                                # 日付や率でないことを確認
                                if not any(skip in clean_key for skip in ['日付', 'Date', '率', 'Rate', '%', 'ID']):
                                    spend_value = test_value
                                    logger.info(f"広告費カラム候補: {clean_key} = {test_value}")
                                    break
                
                if spend_value and spend_value > 0:
                    results[month][f'スポンサープロダクト広告_{account_suffix}'] += spend_value
                    results[month][f'広告費合計_{account_suffix}'] += spend_value
            except Exception as e:
                logger.warning(f"広告費データ行{row_index}処理エラー: {e}")
                continue
        
        # 整数に変換
        for month in results:
            for key in results[month]:
                results[month][key] = int(results[month][key])
        
        return results
    except Exception as e:
        logger.error(f"広告費データ処理エラー: {e}")
        return {}

def merge_monthly_data(all_results):
    """月別データをマージ（スプレッドシート対応）"""
    merged = {}
    
    for source_results in all_results:
        for month, data in source_results.items():
            if month not in merged:
                # スプレッドシートの全項目を初期化
                merged[month] = {
                    'Amazon': 0,
                    'Amazon2': 0,
                    'メルカリShops': 0,
                    
                    # 経費セクション
                    'プラットフォーム手数料_Amazon': 0,
                    'プラットフォーム手数料_Amazon2': 0,
                    'プラットフォーム手数料_メルカリ': 0,
                    '運送費（送料）': 0,
                    
                    '売上総利益': 0,
                    '売上高合計': 0
                }
            
            for key, value in data.items():
                if key in merged[month]:
                    merged[month][key] += value
                else:
                    merged[month][key] = value
    
    return merged

def convert_to_spreadsheet_format(merged_results):
    """マージされたデータをスプレッドシート形式に変換（前月比付き）"""
    spreadsheet_data = []
    previous_row = None
    
    for month_key, data in sorted(merged_results.items()):
        # 年月を日本語形式に変換
        try:
            year, month = month_key.split('-')
            month_display = f"{year}年{int(month)}月"
        except:
            month_display = month_key
        
        # スプレッドシートの行データを作成
        row_data = {
            '年月': month_display,
            '期間': month_key,
            
            # 売上高セクション
            'Amazon（A-M）': data.get('Amazon', 0),
            'Amazon2（O-AA）': data.get('Amazon2', 0), 
            'メルカリShops': data.get('メルカリShops', 0),
            
            # 経費セクション
            'プラットフォーム手数料_Amazon': data.get('プラットフォーム手数料_Amazon', 0),
            'プラットフォーム手数料_Amazon2': data.get('プラットフォーム手数料_Amazon2', 0),
            'プラットフォーム手数料_メルカリ': data.get('プラットフォーム手数料_メルカリ', 0),
            '運送費（送料）': data.get('運送費（送料）', 0),
            
            # 利益セクション
            '売上総利益': data.get('売上総利益', 0),
            '売上高合計': data.get('売上高合計', 0)
        }
        
        # 前月比を計算
        if previous_row:
            sales_change = calculate_change_percentage(
                previous_row['売上高合計'], 
                row_data['売上高合計']
            )
            profit_change = calculate_change_percentage(
                previous_row['売上総利益'], 
                row_data['売上総利益']
            )
            row_data['売上前月比'] = sales_change
            row_data['利益前月比'] = profit_change
        else:
            row_data['売上前月比'] = 0
            row_data['利益前月比'] = 0
        
        spreadsheet_data.append(row_data)
        previous_row = row_data
    
    return spreadsheet_data

def calculate_change_percentage(previous_value, current_value):
    """前月比をパーセンテージで計算"""
    if previous_value == 0:
        return 100 if current_value > 0 else 0  # Infinityの代わりに100%を返す
    result = ((current_value - previous_value) / previous_value) * 100
    # NaNやInfinityのチェック
    if math.isnan(result) or math.isinf(result):
        return 0
    return result

def sanitize_for_json(obj):
    """JSONシリアライズ可能な形式に変換"""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return 0
        return obj
    return obj

@app.route('/')
def index():
    """メインページ"""
    return send_from_directory('static', 'index.html')

@app.route('/api/profit/health', methods=['GET'])
def health_check():
    """ヘルスチェック"""
    return jsonify({
        'status': 'healthy',
        'message': 'Amazon利益計算システム（完全版）が正常に動作しています',
        'version': '2.0.0'
    })

@app.route('/api/profit/upload', methods=['POST'])
def upload_and_calculate():
    """ファイルアップロードと利益計算（月別保存対応）"""
    try:
        # 対象月を取得
        target_month = request.form.get('target_month')
        if not target_month:
            return jsonify({'error': '対象月が指定されていません'}), 400
        
        # アップロードされたファイルを確認
        uploaded_files = {}
        file_paths = {}
        
        file_keys = [
            'makad_a_m', 'hanro_a_m', 'expense_a_m', 'ad_a_m',
            'mercari',
            'makad_o_aa', 'hanro_o_aa', 'expense_o_aa', 'ad_o_aa'
        ]
        
        for key in file_keys:
            if key in request.files:
                file = request.files[key]
                if file and file.filename and allowed_file(file.filename):
                    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                    file.save(temp_file.name)
                    file_paths[key] = temp_file.name
                    uploaded_files[key] = file.filename
                    logger.info(f"ファイルアップロード成功: {key} - {file.filename}")
        
        if not uploaded_files:
            return jsonify({'error': 'ファイルがアップロードされていません'}), 400
        
        # データ処理実行（デバッグ情報付き）
        all_results = []
        debug_info = {}  # デバッグ情報を収集
        
        for key, file_path in file_paths.items():
            try:
                data = safe_read_csv(file_path)
                
                # デバッグ情報を収集（経費・広告費データの場合）
                if ('expense' in key or 'ad' in key) and data and len(data) > 0:
                    debug_info[key] = {
                        'columns': list(data[0].keys())[:20],  # 最初の20カラム
                        'sample_row': {k: str(v)[:50] for k, v in list(data[0].items())[:10]}  # サンプルデータ
                    }
                    logger.info(f"DEBUG {key} columns: {list(data[0].keys())[:10]}")
                
                # アカウントタイプを判定
                account_type = 'o_aa' if 'o_aa' in key else 'a_m'
                
                if 'makad' in key:
                    results = process_makad_data(data, account_type)
                elif 'mercari' in key:
                    results = process_mercari_data(data)
                elif 'hanro' in key:
                    results = process_hanro_data(data, account_type)
                elif 'expense' in key:
                    results = process_expense_data_improved(data, account_type)
                elif 'ad' in key:
                    results = process_ad_data_improved(data, account_type)
                else:
                    continue
                
                if results:
                    all_results.append(results)
                    
            except Exception as e:
                logger.error(f"ファイル処理エラー {key}: {e}")
        
        # 月別データをマージ
        merged_results = merge_monthly_data(all_results)
        
        # スプレッドシート形式に変換
        spreadsheet_data = convert_to_spreadsheet_format(merged_results)
        
        # サマリー計算
        total_sales = sum(data.get('売上高合計', 0) for data in merged_results.values())
        total_profit = sum(data.get('売上総利益', 0) for data in merged_results.values())
        
        summary = {
            'total_months': len(merged_results),
            'months_processed': list(merged_results.keys()),
            'total_sales': total_sales,
            'total_profit': total_profit,
            'average_profit_rate': (total_profit / total_sales * 100) if total_sales > 0 else 0
        }
        
        # ファイルを月別ディレクトリに保存
        saved_files = save_uploaded_files(target_month, file_paths)
        
        # 月別データを保存
        save_success = save_monthly_data(target_month, uploaded_files, merged_results, spreadsheet_data)
        
        # 一時ファイルを安全に削除
        for key, temp_path in file_paths.items():
            try:
                if temp_path and os.path.exists(temp_path):
                    os.unlink(temp_path)
                    logger.debug(f"一時ファイル削除: {key}")
            except PermissionError:
                logger.warning(f"一時ファイル削除権限エラー: {temp_path}")
            except Exception as e:
                logger.error(f"一時ファイル削除エラー: {e}")
        
        # レスポンスを構築
        response = {
            'success': True,
            'message': f'{target_month}のデータを処理し、{len(uploaded_files)}個のファイルを保存しました',
            'target_month': target_month,
            'results': merged_results,
            'spreadsheet_data': spreadsheet_data,
            'summary': summary,
            'uploaded_files': uploaded_files,
            'saved': save_success
        }
        
        # デバッグ情報があれば追加
        if debug_info:
            response['debug_csv_info'] = debug_info
        
        return jsonify(sanitize_for_json(response))
    
    except Exception as e:
        logger.error(f"処理エラー: {e}")
        return jsonify({
            'success': False,
            'error': f'処理中にエラーが発生しました: {str(e)}'
        }), 500

@app.route('/api/profit/validate', methods=['POST'])
def validate_files():
    """ファイル検証のみ実行"""
    try:
        uploaded_files = {}
        file_paths = {}
        
        file_keys = ['makad_a_m', 'mercari', 'hanro_a_m', 'makad_o_aa', 'hanro_o_aa']
        
        for key in file_keys:
            if key in request.files:
                file = request.files[key]
                if file and file.filename and allowed_file(file.filename):
                    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                    file.save(temp_file.name)
                    file_paths[key] = temp_file.name
                    uploaded_files[key] = file.filename
        
        if not uploaded_files:
            return jsonify({'error': 'ファイルがアップロードされていません'}), 400
        
        validation_results = {}
        
        for key, file_path in file_paths.items():
            try:
                data = safe_read_csv(file_path)
                validation_results[key] = {
                    'valid': True,
                    'rows': len(data),
                    'columns': list(data[0].keys()) if data else [],
                    'message': 'ファイル形式が正常です'
                }
            except Exception as e:
                validation_results[key] = {
                    'valid': False,
                    'error': str(e),
                    'message': 'ファイル形式に問題があります'
                }
        
        # 一時ファイルを安全に削除
        for key, temp_path in file_paths.items():
            try:
                if temp_path and os.path.exists(temp_path):
                    os.unlink(temp_path)
                    logger.debug(f"一時ファイル削除: {key}")
            except PermissionError:
                logger.warning(f"一時ファイル削除権限エラー: {temp_path}")
            except Exception as e:
                logger.error(f"一時ファイル削除エラー: {e}")
        
        return jsonify({
            'success': True,
            'validation_results': validation_results,
            'uploaded_files': uploaded_files
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'検証中にエラーが発生しました: {str(e)}'
        }), 500

# 新しいAPIエンドポイントを追加
@app.route('/api/months', methods=['GET'])
def get_months():
    """対象月リストと保存状態を取得"""
    try:
        saved_months = get_saved_months()
        saved_keys = {month['key'] for month in saved_months}
        
        months_with_status = []
        for month in MONTH_LIST:
            months_with_status.append({
                'key': month['key'],
                'display': month['display'],
                'year': month['year'],
                'month': month['month'],
                'has_data': month['key'] in saved_keys
            })
        
        return jsonify({
            'success': True,
            'months': months_with_status,
            'saved_count': len(saved_months)
        })
    except Exception as e:
        logger.error(f"月リスト取得エラー: {e}")
        return jsonify({
            'success': False,
            'error': f'月リスト取得エラー: {str(e)}'
        }), 500

@app.route('/api/months/<month_key>', methods=['GET'])
def get_month_data(month_key):
    """指定月のデータを取得"""
    try:
        data = load_monthly_data(month_key)
        if not data:
            return jsonify({
                'success': False,
                'error': f'{month_key}のデータが見つかりません'
            }), 404
        
        # 月情報を検索
        month_info = next((m for m in MONTH_LIST if m['key'] == month_key), None)
        display_name = month_info['display'] if month_info else month_key
        
        # デバッグ用: 保存されているデータの構造を確認
        debug_info = {}
        if 'results' in data and data['results']:
            # 経費・広告費データのキーを確認
            for month, month_data in data['results'].items():
                for key in month_data.keys():
                    if '経費' in key or '広告' in key or 'A-M' in key or 'O-AA' in key:
                        if month not in debug_info:
                            debug_info[month] = []
                        debug_info[month].append(key)
        
        response = {
            'success': True,
            'month_key': month_key,
            'display_name': display_name,
            'data': data
        }
        
        if debug_info:
            response['debug_keys'] = debug_info
            logger.info(f"保存済みデータのキー構造 ({month_key}): {debug_info}")
        
        return jsonify(response)
    except Exception as e:
        logger.error(f"月データ取得エラー {month_key}: {e}")
        return jsonify({
            'success': False,
            'error': f'月データ取得エラー: {str(e)}'
        }), 500

@app.route('/api/months/<month_key>/spreadsheet', methods=['GET'])
def export_month_spreadsheet(month_key):
    """指定月のスプレッドシートデータをエクスポート"""
    try:
        data = load_monthly_data(month_key)
        if not data:
            return jsonify({
                'success': False,
                'error': f'{month_key}のデータが見つかりません'
            }), 404
        
        # 月情報を検索
        month_info = next((m for m in MONTH_LIST if m['key'] == month_key), None)
        display_name = month_info['display'] if month_info else month_key
        
        return jsonify({
            'success': True,
            'month_key': month_key,
            'display_name': display_name,
            'spreadsheet_data': data['spreadsheet_data'],
            'metadata': data['metadata']
        })
    except Exception as e:
        logger.error(f"スプレッドシートエクスポートエラー {month_key}: {e}")
        return jsonify({
            'success': False,
            'error': f'スプレッドシートエクスポートエラー: {str(e)}'
        }), 500

@app.route('/api/months/<month_key>', methods=['DELETE'])
def delete_month_data(month_key):
    """指定月のデータを削除"""
    try:
        month_dir = os.path.join(DATA_DIR, month_key)
        if os.path.exists(month_dir):
            shutil.rmtree(month_dir)
            logger.info(f"月データ削除完了: {month_key}")
            return jsonify({
                'success': True,
                'message': f'{month_key}のデータを削除しました'
            })
        else:
            return jsonify({
                'success': False,
                'error': f'{month_key}のデータが見つかりません'
            }), 404
    except Exception as e:
        logger.error(f"月データ削除エラー {month_key}: {e}")
        return jsonify({
            'success': False,
            'error': f'月データ削除エラー: {str(e)}'
        }), 500


@app.route('/api/debug/check-saved-data', methods=['GET'])
def debug_check_saved_data():
    """保存済みデータの構造を確認（デバッグ用）"""
    try:
        # 2025-08のデータを確認
        data = load_monthly_data('2025-08')
        if not data:
            return jsonify({'error': '2025-08のデータが見つかりません'}), 404
        
        debug_info = {
            'uploaded_files': data.get('uploaded_files', {}),
            'results_keys': list(data.get('results', {}).keys()) if 'results' in data else [],
            'spreadsheet_columns': [],
            'expense_ad_keys': []
        }
        
        # スプレッドシートデータのカラムを確認
        if 'spreadsheet_data' in data and data['spreadsheet_data']:
            first_row = data['spreadsheet_data'][0] if isinstance(data['spreadsheet_data'], list) else {}
            debug_info['spreadsheet_columns'] = list(first_row.keys())
        
        # 経費・広告費関連のキーを抽出
        if 'results' in data and data['results']:
            for month, month_data in data['results'].items():
                for key in month_data.keys():
                    if any(term in key for term in ['経費', '広告', '仕入', '送料', 'スポンサー', 'A-M', 'O-AA']):
                        if key not in debug_info['expense_ad_keys']:
                            debug_info['expense_ad_keys'].append(key)
        
        logger.info(f"デバッグ情報: {json.dumps(debug_info, ensure_ascii=False, indent=2)}")
        
        return jsonify({
            'success': True,
            'debug_info': debug_info,
            'message': '保存済みデータの構造を確認しました。'
        })
    except Exception as e:
        logger.error(f"デバッグエラー: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/static/<path:filename>')
def serve_static(filename):
    """静的ファイルの配信"""
    return send_from_directory('static', filename)

if __name__ == '__main__':
    debug_mode = os.environ.get('FLASK_ENV') == 'development'
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=debug_mode)

