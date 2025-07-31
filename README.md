# Amazon売上利益計算システム

複数のAmazonアカウントと販路のCSVデータを統合し、月次利益を自動計算するWebアプリケーション。

## 機能

- **マルチファイル対応**: マカド、メルカリショップ、販路プラスの各CSVファイルを同時処理
- **アカウント別管理**: A-MアカウントとO-AAアカウントを分けて管理
- **自動計算**: 売上、手数料、利益を自動集計
- **月次レポート**: 月別の詳細な売上・利益レポートを生成
- **リアルタイム処理**: ファイルアップロード後即座に結果を表示

## デプロイ

### Vercelへのデプロイ

1. GitHubリポジトリをVercelにインポート
2. 自動的にデプロイされます（設定不要）

本アプリケーションはVercel用に最適化されており、`vercel.json`と`api/index.py`により自動設定されます。

## ローカル開発

```bash
# 依存関係のインストール
pip install -r requirements.txt

# 開発サーバー起動
python main.py
```

## 環境変数

```bash
SECRET_KEY=your-secret-key-here  # 本番環境では必須
FLASK_ENV=development           # 開発時のみ
PORT=5000                       # ポート番号（デフォルト: 5000）
```

## API エンドポイント

- `GET /` - メインページ
- `GET /api/profit/health` - ヘルスチェック
- `POST /api/profit/upload` - ファイルアップロードと利益計算
- `POST /api/profit/validate` - ファイル検証のみ

## セキュリティ

- ファイルサイズ制限: 16MB
- 許可ファイル形式: CSV のみ
- 一時ファイルの自動削除
- 適切なエラーハンドリング

## ライセンス

Private - 商用利用