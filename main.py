#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Amazon売上利益計算システム - デプロイ版（軽量）
"""

import os
import tempfile
import logging
import csv
import io
import json
import shutil
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'amazon-profit-calculator-2024')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB

# CORS設定
CORS(app)

# ファイルアップロード設定
ALLOWED_EXTENSIONS = {'csv'}

# データ保存ディレクトリ（Vercel環境では/tmpを使用）
DATA_DIR = '/tmp/monthly_data' if os.environ.get('VERCEL') else 'monthly_data'
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR, exist_ok=True)

# 対象期間の月リストを生成（2025年7月〜2026年7月）
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

def safe_read_csv(file_path):
    """安全なCSV読み込み（軽量版）"""
    try:
        encoding = detect_encoding(file_path)
        
        with open(file_path, 'r', encoding=encoding) as f:
            # CSVを辞書のリストとして読み込み
            reader = csv.DictReader(f)
            data = list(reader)
        
        logger.info(f"ファイル読み込み成功: {file_path} (エンコーディング: {encoding}, 行数: {len(data)})")
        return data
    except Exception as e:
        logger.error(f"ファイル読み込みエラー: {file_path} - {e}")
        raise

def extract_month_from_date(date_str):
    """日付文字列から年月を抽出"""
    try:
        # 様々な日付形式に対応
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) >= 2:
                year = parts[0] if len(parts[0]) == 4 else f"20{parts[0]}"
                month = parts[1].zfill(2)
                return f"{year}-{month}"
        elif '-' in date_str:
            parts = date_str.split('-')
            if len(parts) >= 2:
                return f"{parts[0]}-{parts[1].zfill(2)}"
        
        # デフォルト
        return "2025-06"
    except:
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
                    # 売上高セクション
                    'Amazon' if account_type == 'a_m' else 'Amazon2': 0,
                    'メルカリShops': 0,
                    'バンコ+': 0,
                    'Yahoo!ショッピング': 0,
                    'プリマアプリ': 0,
                    
                    # 経費セクション  
                    'プラットフォーム手数料_Amazon' if account_type == 'a_m' else 'プラットフォーム手数料_Amazon2': 0,
                    'プラットフォーム手数料_メルカリ': 0,
                    '運送費（送料）': 0,
                    
                    # 利益セクション
                    '売上総利益': 0,
                    '売上高合計': 0
                }
            
            # 金額データを抽出
            amazon_key = 'Amazon' if account_type == 'a_m' else 'Amazon2'
            fee_key = 'プラットフォーム手数料_Amazon' if account_type == 'a_m' else 'プラットフォーム手数料_Amazon2'
            
            for key, value in row.items():
                try:
                    num_value = float(str(value).replace(',', '').replace('¥', '')) if value else 0
                    
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
        
        # 整数に変換
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
                    # 売上高セクション
                    'Amazon': 0,
                    'Amazon2': 0,
                    'メルカリShops': 0,
                    'バンコ+': 0,
                    'Yahoo!ショッピング': 0,
                    'プリマアプリ': 0,
                    
                    # 経費セクション
                    'プラットフォーム手数料_Amazon': 0,
                    'プラットフォーム手数料_Amazon2': 0,
                    'プラットフォーム手数料_メルカリ': 0,
                    '運送費（送料）': 0,
                    
                    # 利益セクション
                    '売上総利益': 0,
                    '売上高合計': 0
                }
            
            # 金額データを抽出
            for key, value in row.items():
                try:
                    num_value = float(str(value).replace(',', '').replace('¥', '')) if value else 0
                    
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
        
        # 整数に変換
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
                    # 売上高セクション
                    'Amazon': 0,
                    'Amazon2': 0,
                    'メルカリShops': 0,
                    'バンコ+': 0,
                    'Yahoo!ショッピング': 0,
                    'プリマアプリ': 0,
                    
                    # 経費セクション
                    'プラットフォーム手数料_Amazon': 0,
                    'プラットフォーム手数料_Amazon2': 0,
                    'プラットフォーム手数料_メルカリ': 0,
                    '運送費（送料）': 0,
                    
                    # 利益セクション
                    '売上総利益': 0,
                    '売上高合計': 0
                }
            
            # 販路情報
            mall = row.get('mall', '').lower()
            
            # 金額データを抽出
            for key, value in row.items():
                try:
                    num_value = float(str(value).replace(',', '').replace('¥', '')) if value else 0
                    
                    if 'netPrice' in key or '価格' in key or '売上' in key:
                        if mall == 'rakuten':
                            results[month]['バンコ+'] += num_value
                        elif mall == 'yahoo':
                            results[month]['Yahoo!ショッピング'] += num_value
                        elif mall == 'mercari':
                            results[month]['メルカリShops'] += num_value
                        results[month]['売上高合計'] += num_value
                    elif 'profit' in key or '利益' in key:
                        results[month]['売上総利益'] += num_value
                    elif '送料' in key or 'shipping' in key.lower():
                        results[month]['運送費（送料）'] += num_value
                except:
                    continue
        
        # 整数に変換
        for month in results:
            for key in results[month]:
                results[month][key] = int(results[month][key])
        
        return results
    except Exception as e:
        logger.error(f"販路プラスデータ処理エラー: {e}")
        return {}

def merge_monthly_data(all_results):
    """月別データをマージ（スプレッドシート対応）"""
    merged = {}
    
    for source_results in all_results:
        for month, data in source_results.items():
            if month not in merged:
                # スプレッドシートの全項目を初期化
                merged[month] = {
                    # 売上高セクション
                    'Amazon': 0,
                    'Amazon2': 0,
                    'メルカリShops': 0,
                    'バンコ+': 0,
                    'Yahoo!ショッピング': 0,
                    'プリマアプリ': 0,
                    
                    # 経費セクション
                    'プラットフォーム手数料_Amazon': 0,
                    'プラットフォーム手数料_Amazon2': 0,
                    'プラットフォーム手数料_メルカリ': 0,
                    '運送費（送料）': 0,
                    
                    # 利益セクション
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
            'バンコ+（楽天）': data.get('バンコ+', 0),
            'Yahoo!ショッピング': data.get('Yahoo!ショッピング', 0),
            'プリマアプリ': data.get('プリマアプリ', 0),
            
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
        return float('inf') if current_value > 0 else 0
    return ((current_value - previous_value) / previous_value) * 100

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
            'makad_a_m', 'mercari', 'hanro_a_m',
            'makad_o_aa', 'hanro_o_aa'
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
        
        # データ処理実行
        all_results = []
        
        for key, file_path in file_paths.items():
            try:
                data = safe_read_csv(file_path)
                
                # アカウントタイプを判定
                account_type = 'o_aa' if 'o_aa' in key else 'a_m'
                
                if 'makad' in key:
                    results = process_makad_data(data, account_type)
                elif 'mercari' in key:
                    results = process_mercari_data(data)
                elif 'hanro' in key:
                    results = process_hanro_data(data, account_type)
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
        
        # 一時ファイルを削除
        for temp_path in file_paths.values():
            try:
                os.unlink(temp_path)
            except:
                pass
        
        return jsonify({
            'success': True,
            'message': f'{target_month}のデータを処理し、{len(uploaded_files)}個のファイルを保存しました',
            'target_month': target_month,
            'results': merged_results,
            'spreadsheet_data': spreadsheet_data,
            'summary': summary,
            'uploaded_files': uploaded_files,
            'saved': save_success
        })
    
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
        
        # 一時ファイルを削除
        for temp_path in file_paths.values():
            try:
                os.unlink(temp_path)
            except:
                pass
        
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
        
        return jsonify({
            'success': True,
            'month_key': month_key,
            'display_name': display_name,
            'data': data
        })
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

@app.route('/api/process_multi_account', methods=['POST'])
def process_multi_account():
    """複数アカウント対応のデータ処理エンドポイント"""
    try:
        # アカウントマッピング情報を取得
        account_mapping = json.loads(request.form.get('account_mapping', '{}'))
        
        results = {
            'amazon': {'account1': 0, 'account2': 0, 'total': 0},
            'rakuten': {'account1': 0, 'account2': 0, 'total': 0},
            'yahoo': {'account1': 0, 'account2': 0, 'total': 0},
            'qoo10': {'account1': 0, 'account2': 0, 'total': 0},
            'mercari': {'total': 0}
        }
        
        # 各プラットフォームのファイルを処理
        platforms = ['amazon', 'rakuten', 'yahoo', 'qoo10']
        
        for platform in platforms:
            for account_num in [1, 2]:
                # 売上ファイル処理
                sales_key = f'{platform}-sales-{account_num}'
                expense_key = f'{platform}-expense-{account_num}'
                ad_key = f'{platform}-ad-{account_num}'
                
                account_total = 0
                
                # 売上データ処理
                if sales_key in request.files:
                    file = request.files[sales_key]
                    if file and file.filename:
                        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                        file.save(temp_file.name)
                        
                        try:
                            # CSVデータ読み込み
                            data = safe_read_csv(temp_file.name)
                            
                            # プラットフォーム別の処理
                            if platform == 'amazon':
                                # Amazonの売上処理（既存のロジックを流用）
                                for row in data:
                                    if '売上高' in row:
                                        try:
                                            amount = str(row.get('売上高', '0')).replace(',', '').replace('¥', '')
                                            account_total += float(amount) if amount else 0
                                        except:
                                            pass
                            elif platform == 'rakuten':
                                # 楽天の売上処理
                                for row in data:
                                    if '売上金額' in row or '受注金額' in row:
                                        try:
                                            key = '売上金額' if '売上金額' in row else '受注金額'
                                            amount = str(row.get(key, '0')).replace(',', '').replace('¥', '')
                                            account_total += float(amount) if amount else 0
                                        except:
                                            pass
                            elif platform == 'yahoo':
                                # Yahoo!の売上処理
                                for row in data:
                                    if '売上' in row or '注文金額' in row:
                                        try:
                                            key = '売上' if '売上' in row else '注文金額'
                                            amount = str(row.get(key, '0')).replace(',', '').replace('¥', '')
                                            account_total += float(amount) if amount else 0
                                        except:
                                            pass
                            elif platform == 'qoo10':
                                # Qoo10の売上処理
                                for row in data:
                                    if '決済金額' in row or '販売価格' in row:
                                        try:
                                            key = '決済金額' if '決済金額' in row else '販売価格'
                                            amount = str(row.get(key, '0')).replace(',', '').replace('¥', '')
                                            account_total += float(amount) if amount else 0
                                        except:
                                            pass
                        except Exception as e:
                            logger.error(f"売上データ処理エラー {sales_key}: {e}")
                        finally:
                            os.unlink(temp_file.name)
                
                # 経費データ処理
                if expense_key in request.files:
                    file = request.files[expense_key]
                    if file and file.filename:
                        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                        file.save(temp_file.name)
                        
                        try:
                            data = safe_read_csv(temp_file.name)
                            # 経費を減算
                            for row in data:
                                for key in ['経費', '手数料', 'コスト', '費用']:
                                    if key in row:
                                        try:
                                            amount = str(row.get(key, '0')).replace(',', '').replace('¥', '')
                                            account_total -= float(amount) if amount else 0
                                        except:
                                            pass
                        except Exception as e:
                            logger.error(f"経費データ処理エラー {expense_key}: {e}")
                        finally:
                            os.unlink(temp_file.name)
                
                # 広告費データ処理（Amazonのみ）
                if platform == 'amazon' and ad_key in request.files:
                    file = request.files[ad_key]
                    if file and file.filename:
                        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                        file.save(temp_file.name)
                        
                        try:
                            data = safe_read_csv(temp_file.name)
                            for row in data:
                                if '広告費' in row or 'スポンサー広告費' in row:
                                    try:
                                        key = '広告費' if '広告費' in row else 'スポンサー広告費'
                                        amount = str(row.get(key, '0')).replace(',', '').replace('¥', '')
                                        account_total -= float(amount) if amount else 0
                                    except:
                                        pass
                        except Exception as e:
                            logger.error(f"広告データ処理エラー {ad_key}: {e}")
                        finally:
                            os.unlink(temp_file.name)
                
                # アカウント別の結果を保存
                account_key = f'account{account_num}'
                results[platform][account_key] = account_total
                results[platform]['total'] += account_total
        
        # メルカリShopsの処理
        mercari_key = 'mercari-sales'
        if mercari_key in request.files:
            file = request.files[mercari_key]
            if file and file.filename:
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                file.save(temp_file.name)
                
                try:
                    data = safe_read_csv(temp_file.name)
                    mercari_total = 0
                    for row in data:
                        if '売上金' in row or '売上' in row:
                            try:
                                key = '売上金' if '売上金' in row else '売上'
                                amount = str(row.get(key, '0')).replace(',', '').replace('¥', '')
                                mercari_total += float(amount) if amount else 0
                            except:
                                pass
                    results['mercari']['total'] = mercari_total
                except Exception as e:
                    logger.error(f"メルカリデータ処理エラー: {e}")
                finally:
                    os.unlink(temp_file.name)
        
        # 総合計を計算
        total = sum([
            results['amazon']['total'],
            results['rakuten']['total'],
            results['yahoo']['total'],
            results['qoo10']['total'],
            results['mercari']['total']
        ])
        
        return jsonify({
            'success': True,
            'amazon': results['amazon'],
            'rakuten': results['rakuten'],
            'yahoo': results['yahoo'],
            'qoo10': results['qoo10'],
            'mercari': results['mercari'],
            'total': total,
            'account_mapping': account_mapping
        })
    
    except Exception as e:
        logger.error(f"複数アカウント処理エラー: {e}")
        return jsonify({
            'success': False,
            'error': f'処理中にエラーが発生しました: {str(e)}'
        }), 500

@app.route('/static/<path:path>')
def serve_static(path):
    """静的ファイルを提供"""
    return send_from_directory('static', path)

if __name__ == '__main__':
    debug_mode = os.environ.get('FLASK_ENV') == 'development'
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=debug_mode)

