#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
経費と広告費の処理を改善した関数
実際のCSV構造に基づいた処理
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
    """
    改善された経費データ処理
    実際のカラム構造：
    - Amazonポイントの費用
    - その他の手数料カラム（負の値として記録される可能性）
    - トランザクションの種類による判定
    """
    try:
        results = {}
        
        if data and len(data) > 0:
            logger.info(f"経費データ処理開始 ({account_type}): {len(data)} 行")
            
            # 最初の行のサンプルを出力
            if len(data) > 0:
                sample_row = data[0]
                logger.info(f"経費データサンプル: トランザクションの種類={sample_row.get('トランザクションの種類', 'N/A')}, "
                          f"Amazonポイントの費用={sample_row.get('Amazonポイントの費用', 'N/A')}")
        
        for row_index, row in enumerate(data):
            try:
                # 日付を取得
                date_value = row.get('﻿日付/時間', '') or row.get('日付/時間', '')
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
                
                # トランザクションの種類を確認
                transaction_type = row.get('トランザクションの種類', '')
                description = row.get('説明', '') or row.get('SKU', '')
                
                # Amazonポイントの費用を処理
                point_cost = safe_float_convert(row.get('Amazonポイントの費用', 0))
                if point_cost != 0:
                    results[month][f'ポイント費用_{account_suffix}'] += abs(point_cost)
                    results[month][f'経費合計_{account_suffix}'] += abs(point_cost)
                    if row_index < 3:
                        logger.info(f"ポイント費用検出: {point_cost}円")
                
                # その他の手数料関連フィールドを処理
                # 注文以外のトランザクション（返金、手数料など）を探す
                if transaction_type and '注文' not in transaction_type:
                    # 商品売上（負の値は返金や手数料）
                    product_sales = safe_float_convert(row.get('商品売上', 0))
                    if product_sales < 0:
                        abs_amount = abs(product_sales)
                        
                        # トランザクションタイプで分類
                        if 'FBA' in transaction_type or 'フルフィルメント' in description:
                            results[month][f'FBA手数料_{account_suffix}'] += abs_amount
                        elif '手数料' in transaction_type or 'リファーラル' in transaction_type:
                            results[month][f'Amazon手数料_{account_suffix}'] += abs_amount
                        elif '配送' in transaction_type or '送料' in transaction_type:
                            results[month][f'配送料_{account_suffix}'] += abs_amount
                        else:
                            results[month][f'その他経費_{account_suffix}'] += abs_amount
                        
                        results[month][f'経費合計_{account_suffix}'] += abs_amount
                        
                        if row_index < 3:
                            logger.info(f"手数料検出: {transaction_type} = {product_sales}円 -> {abs_amount}円")
                
                # 配送料の処理（負の値の場合）
                shipping_fee = safe_float_convert(row.get('配送料', 0))
                if shipping_fee < 0:
                    abs_amount = abs(shipping_fee)
                    results[month][f'配送料_{account_suffix}'] += abs_amount
                    results[month][f'経費合計_{account_suffix}'] += abs_amount
                
            except Exception as e:
                logger.warning(f"経費データ行{row_index}処理エラー: {e}")
                continue
        
        # 整数に変換
        for month in results:
            for key in results[month]:
                results[month][key] = int(results[month][key])
        
        # 結果のサマリーをログ出力
        total_expense = sum(v.get(f'経費合計_{account_suffix}', 0) for v in results.values())
        logger.info(f"経費データ処理完了 ({account_type}): {len(results)} ヶ月分, 総経費: {total_expense}円")
        
        return results
        
    except Exception as e:
        logger.error(f"経費データ処理エラー: {e}")
        return {}

def process_ad_data_improved(data, account_type='a_m'):
    """
    改善された広告費データ処理
    実際のカラム構造：
    - 支出
    - 支出 (換算済み)
    """
    try:
        results = {}
        
        if data and len(data) > 0:
            logger.info(f"広告費データ処理開始 ({account_type}): {len(data)} 行")
            
            # 最初の行のサンプルを出力
            if len(data) > 0:
                sample_row = data[0]
                logger.info(f"広告費データサンプル: 支出={sample_row.get('支出', 'N/A')}, "
                          f"支出 (換算済み)={sample_row.get('支出 (換算済み)', 'N/A')}")
        
        for row_index, row in enumerate(data):
            try:
                # 日付を取得（開始日を優先）
                date_value = row.get('開始日', '') or row.get('終了日', '')
                if not date_value:
                    # BOM付きのカラムも試す
                    date_value = row.get('﻿開始日', '') or row.get('﻿終了日', '')
                
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
                
                # 支出を取得（換算済みを優先）
                spend_value = 0
                
                # 「支出 (換算済み)」を最初に試す
                spend_converted = row.get('支出 (換算済み)', '')
                if spend_converted:
                    spend_value = safe_float_convert(spend_converted)
                
                # 値がない場合は「支出」を試す
                if spend_value == 0:
                    spend_normal = row.get('支出', '')
                    if spend_normal:
                        spend_value = safe_float_convert(spend_normal)
                
                # 広告費を集計
                if spend_value > 0:
                    results[month][f'スポンサープロダクト広告_{account_suffix}'] += spend_value
                    results[month][f'広告費合計_{account_suffix}'] += spend_value
                    
                    if row_index < 3:
                        logger.info(f"広告費検出: キャンペーン={row.get('キャンペーン', 'N/A')}, 支出={spend_value}円")
                
            except Exception as e:
                logger.warning(f"広告費データ行{row_index}処理エラー: {e}")
                continue
        
        # 整数に変換
        for month in results:
            for key in results[month]:
                results[month][key] = int(results[month][key])
        
        # 結果のサマリーをログ出力
        total_ad_spend = sum(v.get(f'広告費合計_{account_suffix}', 0) for v in results.values())
        logger.info(f"広告費データ処理完了 ({account_type}): {len(results)} ヶ月分, 総広告費: {total_ad_spend}円")
        
        return results
        
    except Exception as e:
        logger.error(f"広告費データ処理エラー: {e}")
        return {}