#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
経費と広告費の処理を改善した関数
"""

import logging

logger = logging.getLogger(__name__)

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
    """日付文字列から年月を抽出"""
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

def process_expense_data_improved(data, account_type='a_m'):
    """改善された経費データ処理"""
    try:
        results = {}
        
        if data and len(data) > 0:
            all_columns = list(data[0].keys())
            logger.info(f"経費データカラム ({account_type}): 合計 {len(all_columns)} カラム")
            
            # デバッグ: 負の値を持つカラムを探す
            negative_value_columns = set()
            for row in data[:10]:  # 最初の10行をチェック
                for key, value in row.items():
                    if safe_float_convert(value) < 0:
                        negative_value_columns.add(key)
            
            if negative_value_columns:
                logger.info(f"負の値を持つカラム: {list(negative_value_columns)[:10]}")
        
        for row_index, row in enumerate(data):
            try:
                # 日付を探す
                date_value = None
                for key, value in row.items():
                    clean_key = key.replace('﻿', '').replace('\ufeff', '').strip()
                    if any(d in clean_key for d in ['日付', '時間', 'date', 'Date', 'Time']):
                        if value:
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
                
                account_suffix = 'A-M' if account_type == 'a_m' else 'O-AA'
                
                # すべてのカラムから負の値（手数料）を探す
                for key, value in row.items():
                    if not value or value == '':
                        continue
                    
                    clean_key = key.replace('﻿', '').replace('\ufeff', '').strip()
                    num_value = safe_float_convert(value)
                    
                    # 負の値は手数料・費用
                    if num_value < 0:
                        abs_amount = abs(num_value)
                        
                        # トランザクションタイプと説明を取得
                        transaction_type = row.get('トランザクションの種類', '')
                        description = row.get('商品の説明', '')
                        combined = f"{clean_key} {transaction_type} {description}".lower()
                        
                        # 分類
                        if any(term in combined for term in ['fba', 'フルフィルメント', '在庫', '保管', '出荷']):
                            results[month][f'FBA手数料_{account_suffix}'] += abs_amount
                        elif any(term in combined for term in ['成約', 'リファーラル', '販売手数料', 'commission']):
                            results[month][f'Amazon手数料_{account_suffix}'] += abs_amount
                        elif any(term in combined for term in ['配送', 'shipping', '送料', '発送']):
                            results[month][f'配送料_{account_suffix}'] += abs_amount
                        elif any(term in combined for term in ['ポイント', 'point']):
                            results[month][f'ポイント費用_{account_suffix}'] += abs_amount
                        else:
                            results[month][f'その他経費_{account_suffix}'] += abs_amount
                        
                        results[month][f'経費合計_{account_suffix}'] += abs_amount
                        
                        # 最初の数行だけログ出力
                        if row_index < 3:
                            logger.info(f"経費検出: {clean_key} = {num_value} -> {abs_amount}円")
                
            except Exception as e:
                logger.warning(f"経費データ行{row_index}処理エラー: {e}")
                continue
        
        # 整数に変換
        for month in results:
            for key in results[month]:
                results[month][key] = int(results[month][key])
        
        logger.info(f"経費データ処理完了: {len(results)} ヶ月分")
        return results
        
    except Exception as e:
        logger.error(f"経費データ処理エラー: {e}")
        return {}

def process_ad_data_improved(data, account_type='a_m'):
    """改善された広告費データ処理"""
    try:
        results = {}
        
        if data and len(data) > 0:
            all_columns = list(data[0].keys())
            logger.info(f"広告費データカラム ({account_type}): 合計 {len(all_columns)} カラム")
            
            # デバッグ: 正の値を持つカラムを探す
            positive_value_columns = {}
            for row in data[:10]:  # 最初の10行をチェック
                for key, value in row.items():
                    val = safe_float_convert(value)
                    if 0 < val < 1000000:  # 妥当な範囲の正の値
                        if key not in positive_value_columns:
                            positive_value_columns[key] = []
                        positive_value_columns[key].append(val)
            
            # 広告費の候補カラムをログ出力
            for col, values in positive_value_columns.items():
                clean_col = col.replace('﻿', '').replace('\ufeff', '').strip()
                if any(term in clean_col for term in ['支出', 'Spend', '費用', 'Cost', '広告']):
                    logger.info(f"広告費候補カラム: '{col}' サンプル値: {values[:3]}")
        
        for row_index, row in enumerate(data):
            try:
                # 日付を探す
                date_value = None
                for key, value in row.items():
                    clean_key = key.replace('﻿', '').replace('\ufeff', '').strip()
                    if any(d in clean_key for d in ['日付', '開始', '終了', 'date', 'Date', 'Start', 'End']):
                        if value:
                            date_value = value
                            break
                
                if not date_value:
                    # 日付形式の値を探す
                    for value in row.values():
                        if value and ('/' in str(value) or '-' in str(value)):
                            try:
                                test_month = extract_month_from_date(str(value))
                                if test_month != "2025-06":
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
                
                # 広告費を探す
                ad_spend_found = False
                for key, value in row.items():
                    if not value or value == '' or ad_spend_found:
                        continue
                    
                    clean_key = key.replace('﻿', '').replace('\ufeff', '').strip()
                    num_value = safe_float_convert(value)
                    
                    # 正の値で広告費関連のカラム
                    if num_value > 0:
                        if any(term in clean_key for term in ['支出', 'Spend', '費用', 'Cost', '広告費', 'Ad']):
                            # 除外するカラム
                            if not any(skip in clean_key for skip in ['日付', 'Date', '率', 'Rate', '%', 'ID', 'インプレッション', 'クリック数', '売上']):
                                results[month][f'スポンサープロダクト広告_{account_suffix}'] += num_value
                                results[month][f'広告費合計_{account_suffix}'] += num_value
                                ad_spend_found = True
                                
                                # 最初の数行だけログ出力
                                if row_index < 3:
                                    logger.info(f"広告費検出: {clean_key} = {num_value}円")
                
            except Exception as e:
                logger.warning(f"広告費データ行{row_index}処理エラー: {e}")
                continue
        
        # 整数に変換
        for month in results:
            for key in results[month]:
                results[month][key] = int(results[month][key])
        
        logger.info(f"広告費データ処理完了: {len(results)} ヶ月分")
        return results
        
    except Exception as e:
        logger.error(f"広告費データ処理エラー: {e}")
        return {}