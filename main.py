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
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static')
app.config['SECRET_KEY'] = 'amazon-profit-calculator-2024'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB

# CORS設定
CORS(app)

# ファイルアップロード設定
ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(filename):
    """許可されたファイル形式かチェック"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

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

def process_makad_data(data):
    """マカドデータ処理（軽量版）"""
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
                    'プラットフォーム手数料_Amazon': 0,
                    '売上総利益': 0,
                    '売上高合計': 0
                }
            
            # 金額データを抽出
            for key, value in row.items():
                try:
                    num_value = float(str(value).replace(',', '').replace('¥', '')) if value else 0
                    
                    if '販売価格' in key or '売上' in key:
                        results[month]['Amazon'] += num_value
                        results[month]['売上高合計'] += num_value
                    elif '手数料' in key:
                        results[month]['プラットフォーム手数料_Amazon'] += num_value
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
    """メルカリショップデータ処理（軽量版）"""
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
                    'メルカリShops': 0,
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
                    elif '利益' in key:
                        results[month]['売上総利益'] += num_value
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

def process_hanro_data(data):
    """販路プラスデータ処理（軽量版）"""
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
                    'バンコ+': 0,
                    'Yahoo!ショッピング': 0,
                    '売上総利益': 0,
                    '売上高合計': 0
                }
            
            # 販路情報
            mall = row.get('mall', '')
            
            # 金額データを抽出
            for key, value in row.items():
                try:
                    num_value = float(str(value).replace(',', '').replace('¥', '')) if value else 0
                    
                    if 'netPrice' in key or '価格' in key:
                        if mall == 'rakuten':
                            results[month]['バンコ+'] += num_value
                        elif mall == 'yahoo':
                            results[month]['Yahoo!ショッピング'] += num_value
                        results[month]['売上高合計'] += num_value
                    elif 'profit' in key or '利益' in key:
                        results[month]['売上総利益'] += num_value
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
    """月別データをマージ"""
    merged = {}
    
    for source_results in all_results:
        for month, data in source_results.items():
            if month not in merged:
                merged[month] = {}
            
            for key, value in data.items():
                if key in merged[month]:
                    merged[month][key] += value
                else:
                    merged[month][key] = value
    
    return merged

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
    """ファイルアップロードと利益計算"""
    try:
        # アップロードされたファイルを確認
        uploaded_files = {}
        file_paths = {}
        
        file_keys = [
            'makad_a_m', 'mercari_a_m', 'hanro_a_m',
            'makad_o_aa', 'mercari_o_aa', 'hanro_o_aa'
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
                
                if 'makad' in key:
                    results = process_makad_data(data)
                elif 'mercari' in key:
                    results = process_mercari_data(data)
                elif 'hanro' in key:
                    results = process_hanro_data(data)
                else:
                    continue
                
                if results:
                    all_results.append(results)
                    
            except Exception as e:
                logger.error(f"ファイル処理エラー {key}: {e}")
        
        # 月別データをマージ
        merged_results = merge_monthly_data(all_results)
        
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
        
        # 一時ファイルを削除
        for temp_path in file_paths.values():
            try:
                os.unlink(temp_path)
            except:
                pass
        
        return jsonify({
            'success': True,
            'message': f'{len(uploaded_files)}個のファイルを処理しました',
            'results': merged_results,
            'summary': summary,
            'uploaded_files': uploaded_files
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
        
        file_keys = ['makad_a_m', 'mercari_a_m', 'hanro_a_m', 'makad_o_aa', 'mercari_o_aa', 'hanro_o_aa']
        
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

